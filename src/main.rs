use tokio::{net::TcpListener, sync::Mutex, io::{self, BufReader, AsyncBufReadExt}};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};
use dotenv::dotenv;
use std::{
    collections::HashMap,
    env,
    sync::Arc,
};
use serde::{Serialize, Deserialize};
use inquire::{Select, Text};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ClientMsg {
    pc: u8,
    id: String,
    score: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ServerMsg {
    pc: u8,
    id: String,
    score: u32,
    ranking: u16,
}

#[derive(Debug, Clone)]
struct FinalScoreEntry {
    id: String,
    score: u32,
    name: Option<String>,
}

/// ランキングを計算する（高スコア順）
fn calculate_ranking(final_scores: &Vec<FinalScoreEntry>, target_id: &str) -> u16 {
    let mut sorted = final_scores.clone();
    sorted.sort_by(|a, b| b.score.cmp(&a.score)); // スコア降順
    for (i, entry) in sorted.iter().enumerate() {
        if entry.id == target_id {
            return (i + 1) as u16;
        }
    }
    0
}

#[tokio::main]
async fn main() {
    dotenv().expect("🚫 .env file not found");

    // ステージ中のスコア一時保存（id → 合計点）
    let temp_scores: Arc<Mutex<HashMap<String, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    // 終了したチームのスコア記録（FinalScoreEntryのVec）
    let final_scores: Arc<Mutex<Vec<FinalScoreEntry>>> = Arc::new(Mutex::new(Vec::new()));

    {
        let final_scores = Arc::clone(&final_scores);
        tokio::spawn(async move {
            let stdin = io::stdin();
            let reader = BufReader::new(stdin);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let line_trimmed = line.trim();
                match line_trimmed.to_lowercase().as_str() {
                    "ranking" => {
                        let finals = final_scores.lock().await;
                        let mut sorted = finals.clone();
                        sorted.sort_by(|a, b| b.score.cmp(&a.score));

                        println!("--- Current Ranking ---");
                        for (i, entry) in sorted.iter().enumerate() {
                            let display_name = entry.name.as_deref().unwrap_or("(Unnamed)");
                            println!("{}. [{}] {} : {}pt", i + 1, entry.id, display_name, entry.score);
                        }
                        println!("------------------------");
                    }
                    "exit" => {
                        println!("🛑 Server shutting down...");
                        std::process::exit(0);
                    }
                    "name" => {
                        // nameコマンド処理は同期のinquireなので
                        // Tokioのblock_in_placeで囲む
                        tokio::task::block_in_place(|| {
                            let rt = tokio::runtime::Handle::current();
                            rt.block_on(async {
                                let mut finals = final_scores.lock().await;
                                if finals.is_empty() {
                                    println!("⚠ No final scores available to name.");
                                    return;
                                }

                                // トップ10をスコア順で抽出
                                let mut sorted = finals.clone();
                                sorted.sort_by(|a, b| b.score.cmp(&a.score));
                                let top_n = sorted.iter().take(10).cloned().collect::<Vec<_>>();

                                // 選択肢文字列の生成
                                let options: Vec<String> = top_n.iter()
                                    .enumerate()
                                    .map(|(idx, entry)| {
                                        let display_name = entry.name.as_deref().unwrap_or("(Unnamed)");
                                        format!("{}: [{}] {} - {}pt", idx + 1, entry.id, display_name, entry.score)
                                    })
                                    .collect();

                                if options.is_empty() {
                                    println!("⚠ No entries to name.");
                                    return;
                                }

                                let selection = Select::new("Select an entry to name:", options)
                                    .prompt();

                                match selection {
                                    Ok(selected_str) => {
                                        // 選択された文字列の先頭番号をパースしてインデックスを取得
                                        let selected_index = selected_str.split(':').next()
                                            .and_then(|s| s.parse::<usize>().ok())
                                            .map(|i| i - 1);

                                        if let Some(idx) = selected_index {
                                            if idx >= top_n.len() {
                                                println!("⚠ Invalid selection index.");
                                                return;
                                            }
                                            // 元のfinal_scoresのインデックスを探す
                                            let target_entry = &top_n[idx];
                                            let pos = finals.iter().position(|e| e.id == target_entry.id && e.score == target_entry.score && e.name == target_entry.name);
                                            if let Some(pos) = pos {
                                                // 名前入力
                                                match Text::new("Enter a name:")
                                                    .with_placeholder("Team Name")
                                                    .prompt() {
                                                    Ok(name_input) => {
                                                        finals[pos].name = Some(name_input.clone());
                                                        println!("Name '{}' assigned to [{}] {}pt.", name_input, finals[pos].id, finals[pos].score);
                                                    }
                                                    Err(_) => {
                                                        println!("⚠ Name input cancelled or failed.");
                                                    }
                                                }
                                            } else {
                                                println!("⚠ Selected entry not found in final scores.");
                                            }
                                        } else {
                                            println!("⚠ Could not parse selection index.");
                                        }
                                    }
                                    Err(_) => {
                                        println!("⚠ Selection cancelled or failed.");
                                    }
                                }
                            })
                        });
                    }
                    "help" => {
                        println!("ranking: Show the Ranking\nname: Name Records of the Ranking\nexit: Exit the System")
                    }
                    other => {
                        println!("⚠ Unknown command: {}", other);
                        println!("Available commands: ranking, name, exit");
                    }
                }
            }
        });
    }

    let ip = env::var("IP").expect("🚫 IP not set");
    println!("WebSocket > ws://{}:9001", ip);
    let listener = TcpListener::bind("0.0.0.0:9001").await.unwrap();

    while let Ok((stream, _)) = listener.accept().await {
        let temp_scores = Arc::clone(&temp_scores);
        let final_scores = Arc::clone(&final_scores);

        tokio::spawn(async move {
            let ws_stream = accept_async(stream).await.unwrap();
            let (mut write, mut read) = ws_stream.split();
            println!("-- Connected Successfully --");
            write.send(Message::Text("connected".to_owned())).await.unwrap();

            while let Some(msg) = read.next().await {
                if let Ok(msg) = msg {
                    if let Message::Text(text) = msg {
                        if let Ok(client_msg) = serde_json::from_str::<ClientMsg>(&text) {
                            let log = &client_msg;
                            let mut temp = temp_scores.lock().await;

                            // スコアを加算
                            let score_entry = temp.entry(client_msg.id.clone()).or_insert(0);
                            *score_entry += client_msg.score;

                            println!(
                                "Record [{}]: Stage{} - {}pt (+{}pt)",
                                log.id, log.pc, score_entry, client_msg.score
                            );

                            // ステージ3終了 → 最終結果送信
                            if client_msg.pc == 3 {
                                let total = *score_entry;

                                // final_scores に記録
                                drop(temp); // lock解放順に注意
                                {
                                    let mut finals = final_scores.lock().await;
                                    finals.push(FinalScoreEntry {
                                        id: client_msg.id.clone(),
                                        score: total,
                                        name: None,
                                    });
                                }

                                // ランキング算出
                                let finals = final_scores.lock().await;
                                let ranking = calculate_ranking(&finals, &client_msg.id);
                                drop(finals);

                                let server_msg = ServerMsg {
                                    pc: 4,
                                    id: client_msg.id.clone(),
                                    score: total,
                                    ranking,
                                };
                                let json = serde_json::to_string(&server_msg).unwrap();

                                println!(
                                    "Result [{}]: {}pt, Rank: {}",
                                    log.id, total, ranking
                                );
                                write.send(Message::Text(json)).await.unwrap();

                                // temp_scores から削除
                                let mut temp = temp_scores.lock().await;
                                temp.remove(&client_msg.id);
                                println!("Remove [{}]", log.id);
                            }
                        } else {
                            println!("⚠ Invalid JSON: {}", text);
                        }
                    }
                }
            }
        });
    }
}

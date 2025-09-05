use tokio::{net::TcpListener, sync::Mutex, io::{self, BufReader, AsyncBufReadExt}};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{StreamExt, SinkExt};
use dotenv::dotenv;

use std::{
    collections::HashMap, env, fs::File, io::{Read, Write}, sync::{atomic::{AtomicU16, Ordering}, Arc}
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FinalScoreEntry {
    idx: u16,           // 内部識別用
    id: String,         // クライアントID（表示用）
    score: u32,
    name: Option<String>,
}

/// ランキングを計算する（高スコア順）
fn calculate_ranking(final_scores: &Vec<FinalScoreEntry>, target_idx: u16) -> u16 {
    let mut sorted = final_scores.clone();
    sorted.sort_by(|a, b| b.score.cmp(&a.score)); // スコア降順
    for (i, entry) in sorted.iter().enumerate() {
        if entry.idx == target_idx {
            return (i + 1) as u16;
        }
    }
    0
}

fn set_current_dir_to_exe_location() {
    if let Ok(exe_path) = env::current_exe() {
        if let Some(parent) = exe_path.parent() {
            let _ = env::set_current_dir(parent);
        }
    }
}

#[tokio::main]
async fn main() {
    set_current_dir_to_exe_location();
    dotenv().expect("🚫 .env file not found");

    let temp_scores: Arc<Mutex<HashMap<String, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    let final_scores: Arc<Mutex<Vec<FinalScoreEntry>>> = Arc::new(Mutex::new(Vec::new()));
    let idx_counter: Arc<AtomicU16> = Arc::new(AtomicU16::new(0));

    {
        let final_scores = Arc::clone(&final_scores);
        tokio::spawn(async move {
            let stdin = io::stdin();
            let reader = BufReader::new(stdin);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                let line_trimmed = line.trim();
                match line_trimmed.to_lowercase().as_str() {
                    "ranking"|"r" => {
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
                    "clear!" => {
                        let mut finals = final_scores.lock().await;
                        finals.clear();
                        println!("Ranking removed.")
                    }
                    "save"|"s" => {
                        let finals = final_scores.lock().await;
                        let json = serde_json::to_string_pretty(&*finals).unwrap();
                        if let Ok(mut file) = File::create("final_scores.json") {
                            if file.write_all(json.as_bytes()).is_ok() {
                                println!("💾 Rankings saved to final_scores.json");
                            } else {
                                println!("❌ Failed to write to file.");
                            }
                        } else {
                            println!("❌ Failed to create file.");
                        }
                    }
                    "load"|"l" => {
                        let mut finals = final_scores.lock().await;

                        match tokio::task::spawn_blocking(|| {
                            let mut file = File::open("final_scores.json")
                                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
                            
                            let mut contents = String::new();
                            file.read_to_string(&mut contents)
                                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
                            
                            let parsed: Vec<FinalScoreEntry> = serde_json::from_str(&contents)
                                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send>)?;
                            
                            Ok::<_, Box<dyn std::error::Error + Send>>(parsed)
                        })
                        .await
                        {
                            Ok(Ok(loaded_scores)) => {
                                *finals = loaded_scores;
                                println!("✅ Rankings loaded from final_scores.json");
                            }
                            Ok(Err(e)) => {
                                println!("❌ Failed to load rankings: {}", e);
                            }
                            Err(e) => {
                                println!("❌ Task Join Error: {}", e);
                            }
                        }
                    }     
                    "count"|"c" => {
                        let finals = final_scores.lock().await;
                        println!("{}",finals.iter().len())
                    } 
                    "exit!" => {
                        println!("🛑 Server shutting down...");
                        std::process::exit(0);
                    }
                    "name"|"n" => {
                        tokio::task::block_in_place(|| {
                            let rt = tokio::runtime::Handle::current();
                            rt.block_on(async {
                                let mut finals = final_scores.lock().await;
                                if finals.is_empty() {
                                    println!("⚠ No final scores available to name.");
                                    return;
                                }

                                let mut sorted = finals.clone();
                                sorted.sort_by(|a, b| b.score.cmp(&a.score));
                                let top_n = sorted.iter().take(10).cloned().collect::<Vec<_>>();

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
                                        let selected_index = selected_str.split(':').next()
                                            .and_then(|s| s.parse::<usize>().ok())
                                            .map(|i| i - 1);

                                        if let Some(idx) = selected_index {
                                            if idx >= top_n.len() {
                                                println!("⚠ Invalid selection index.");
                                                return;
                                            }
                                            let target_entry = &top_n[idx];
                                            let pos = finals.iter().position(|e| e.idx == target_entry.idx);
                                            if let Some(pos) = pos {
                                                match Text::new("Enter a name:")
                                                    .with_placeholder("Team Name")
                                                    .prompt() {
                                                    Ok(name_input) => {
                                                        if name_input != "" {
                                                            finals[pos].name = Some(name_input.clone());
                                                            println!("Name '{}' assigned to [{}] {}pt.", name_input, finals[pos].id, finals[pos].score);
                                                        }else {
                                                            println!("Canceled.")
                                                        }
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
                    "help"|"h" => {
                        println!("ranking: Show the Ranking\nname: Name Records of the Ranking\nsave: Save the Ranking\nload: Load the Ranking\ncount: Show Counter\nclear!: Clear the Ranking\nexit!: Exit the System\n\n(Initial-Char Input Enabled.)")
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
    let listener = TcpListener::bind("172.17.98.44:9001").await.unwrap();

    while let Ok((stream, _)) = listener.accept().await {
        let temp_scores = Arc::clone(&temp_scores);
        let final_scores = Arc::clone(&final_scores);
        let idx_counter = Arc::clone(&idx_counter);

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

                            let score_entry = temp.entry(client_msg.id.clone()).or_insert(0);
                            *score_entry += client_msg.score;

                            println!(
                                "Record [{}]: Stage{} - {}pt (+{}pt)",
                                log.id, log.pc, score_entry, client_msg.score
                            );

                            if client_msg.pc == 3 {
                                let total = *score_entry;
                                drop(temp);

                                let idx = idx_counter.fetch_add(1, Ordering::Relaxed);

                                {
                                    let mut finals = final_scores.lock().await;
                                    finals.push(FinalScoreEntry {
                                        idx,
                                        id: client_msg.id.clone(),
                                        score: total,
                                        name: None,
                                    });
                                }

                                let finals = final_scores.lock().await;
                                let ranking = calculate_ranking(&finals, idx);
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

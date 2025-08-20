use tokio::{net::TcpListener,sync::Mutex};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use futures_util::{StreamExt,SinkExt};
use dotenv::dotenv;
use std::{collections::HashMap, env, sync::{Arc}};
use serde::{Serialize,Deserialize};
// use serde_json

#[derive(Serialize,Deserialize,Debug,Clone)]
struct ClientMsg{
    pc: u8,
    id: String,
    score: u32
}
#[derive(Serialize,Deserialize,Debug,Clone)]
struct ServerMsg{
    pc: u8,
    id: String,
    score: u32,
    ranking: u16
}

fn calculate_ranking(team_data: &HashMap<String, u32>, target_id: &str) -> u16 {
    let mut sorted: Vec<_> = team_data.iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(a.1)); // スコアの降順

    for (i, (id, _)) in sorted.iter().enumerate() {
        if *id == target_id {
            return (i + 1) as u16;
        }
    }
    0
}


#[tokio::main]
async fn main() {
    let team_data:Arc<Mutex<HashMap<String,u32>>>        = Arc::new(Mutex::new(HashMap::new()));
    dotenv().expect("🚫 .env file not found");
    for (key,val) in env::vars(){
        if key == "IP"{
            println!("WebSocket >  ws://{}:9001",val);
            let listener = TcpListener::bind("0.0.0.0:9001").await.unwrap();
            while let Ok((stream,_)) = listener.accept().await {
                let team_data = Arc::clone(&team_data);
                tokio::spawn(async move {
                    let ws_stream = accept_async(stream).await.unwrap();
                    let (mut write, mut read) = ws_stream.split();
                    println!("-- Connected Successfully --");
                    write.send(Message::Text("connected".to_owned())).await.unwrap();
                    while let Some(msg) = read.next().await {
                        if let Ok(msg) = msg{     
                            if let Message::Text(text) = msg {
                                if let Ok(client_msg) = serde_json::from_str::<ClientMsg>(&text) {
                                    let log = &client_msg;
                                    let mut data = team_data.lock().await;
                                    let calc = data.clone();
                                    let score = data.entry(client_msg.id.clone()).or_insert(0);
                                    println!("Record: Game{} - {} team - {}pt(+{}pt)",log.pc,log.id,log.score,&score);
                                    *score += client_msg.score;
                                    if client_msg.pc==3 {
                                        let ranking = calculate_ranking(&calc, &client_msg.id);
                                        if ranking==0 {
                                            println!("⚠ Inconsistency occurred...")
                                        }
                                        let server_msg = ServerMsg {
                                            pc: 4,
                                            id: client_msg.id.clone(),
                                            score:*score,
                                            ranking
                                        };
                                        let json = serde_json::to_string(&server_msg).unwrap();
                                        println!("Result: {} team - {}pt", log.id, log.score);
                                        write.send(Message::Text(json)).await.unwrap();
                                        data.remove(&client_msg.id);
                                        println!("Remove: {} team", log.id);
                                    }
                                } else {
                                    println!("⚠ Invalid JSON: {}", text);
                                }
                            }
                        }
                    }
                });
            }
            break;
        }
    }
}

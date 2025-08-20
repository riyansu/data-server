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


#[tokio::main]
async fn main() {
    let team_data:Arc<Mutex<HashMap<String,u32>>>        = Arc::new(Mutex::new(HashMap::new()));
    dotenv().expect(".env file not found");
    for (key,val) in env::vars(){
        if key == "IP"{
            println!("WebSocket >  ws://{}:9001",val);
            let listener = TcpListener::bind("0.0.0.0:9001").await.unwrap();
            while let Ok((stream,_)) = listener.accept().await {
                let team_data = Arc::clone(&team_data);
                tokio::spawn(async move {
                    let ws_stream = accept_async(stream).await.unwrap();
                    let (mut write, mut read) = ws_stream.split();
                    println!("connected");
                    write.send(Message::Text("connected".to_owned())).await.unwrap();
                    while let Some(msg) = read.next().await {
                        if let Ok(msg) = msg{     
                            if let Message::Text(text) = msg {
                                // JSONをClientMessageに変換
                                if let Ok(client_msg) = serde_json::from_str::<ClientMsg>(&text) {
                                    let mut data = team_data.lock().await;
                                    let score = data.entry(client_msg.id.clone()).or_insert(0);
                                    *score += client_msg.score;
                                    if client_msg.pc==3 {
                                        let server_msg = ServerMsg {
                                            pc: 4,
                                            id: client_msg.id.clone(),
                                            score:*score,
                                            ranking:1,
                                        };
                                        let json = serde_json::to_string(&server_msg).unwrap();
                                        write.send(Message::Text(json)).await.unwrap();
                                    }
                                } else {
                                    println!("Invalid JSON: {}", text);
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

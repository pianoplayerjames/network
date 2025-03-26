use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, BufReader, BufRead};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use serde::{Serialize, Deserialize};

type PeerId = String;

// Configuration struct to hold settings
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Config {
    bootstrap_nodes: Vec<String>,
    discovery_port: u16,
    tcp_port: u16,
    max_hop_count: u32,
    peer_exchange_interval: u64,
    maintenance_interval: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            bootstrap_nodes: vec![
                "bootstrap1.example.com:33445".to_string(),
                "198.51.100.1:33445".to_string(),
            ],
            discovery_port: 33445,
            tcp_port: 33445,
            max_hop_count: 10,
            peer_exchange_interval: 60,
            maintenance_interval: 300,
        }
    }
}

// Load config from file or create default
fn load_config(file_path: &str) -> Config {
    match File::open(file_path) {
        Ok(file) => {
            let reader = BufReader::new(file);
            match serde_json::from_reader(reader) {
                Ok(config) => {
                    println!("Loaded configuration from {}", file_path);
                    config
                }
                Err(e) => {
                    println!("Error parsing config file: {}. Using default config.", e);
                    let config = Config::default();
                    // Try to save the default config
                    let _ = save_config(file_path, &config);
                    config
                }
            }
        }
        Err(_) => {
            println!("Config file not found. Creating default at {}", file_path);
            let config = Config::default();
            let _ = save_config(file_path, &config);
            config
        }
    }
}

// Save config to file
fn save_config(file_path: &str, config: &Config) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_path)?;
    
    let writer = std::io::BufWriter::new(file);
    match serde_json::to_writer_pretty(writer, config) {
        Ok(_) => println!("Configuration saved to {}", file_path),
        Err(e) => println!("Error saving configuration: {}", e),
    }
    
    Ok(())
}

// Custom serialization for HashSet for serde
mod hashset_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::HashSet;
    use std::hash::Hash;

    pub fn serialize<S, T>(value: &HashSet<T>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize + Hash + Eq,
    {
        let vec: Vec<&T> = value.iter().collect();
        vec.serialize(serializer)
    }

    pub fn deserialize<'de, D, T>(deserializer: D) -> Result<HashSet<T>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de> + Hash + Eq,
    {
        let vec: Vec<T> = Vec::deserialize(deserializer)?;
        Ok(vec.into_iter().collect())
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct Message {
    id: String,
    source: PeerId,
    destination: Option<PeerId>,
    content: Vec<u8>,
    hop_count: u32,
    #[serde(with = "hashset_serde")]
    seen_by: HashSet<PeerId>,
}

enum MessageType {
    Regular,
    Discovery,
    PeerList,
    PeerExchange,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct PeerInfo {
    addr: SocketAddr,
    id: PeerId,
    last_seen: u64, // Timestamp
}

struct Node {
    id: PeerId,
    listener: TcpListener,
    discovery_socket: Arc<UdpSocket>,
    address: SocketAddr,
    peers: Arc<Mutex<HashMap<PeerId, TcpStream>>>,
    known_peers: Arc<Mutex<HashMap<PeerId, PeerInfo>>>,
    bootstrap_nodes: Vec<SocketAddr>,
    message_cache: Arc<Mutex<HashSet<String>>>,
    max_hop_count: u32,
    running: Arc<Mutex<bool>>,
    peer_file_path: String,
    config: Config,
}

impl Node {
    fn new(bind_addr: &str, config_path: &str) -> std::io::Result<Self> {
        // Load configuration
        let config = load_config(config_path);
        
        let listener = TcpListener::bind(bind_addr)?;
        let address = listener.local_addr()?;
        let id = format!("node_{}", address.to_string().replace(":", "_"));
        
        let ip_str = if address.ip().is_unspecified() { 
            "0.0.0.0".to_string() 
        } else { 
            address.ip().to_string() 
        };
        let discovery_addr = format!("{}:{}", ip_str, config.discovery_port);
        let discovery_socket = UdpSocket::bind(discovery_addr)?;
        discovery_socket.set_broadcast(true)?;
        
        // Path for storing known peers
        let peer_file_path = format!("{}_peers.txt", id);
        
        // Convert bootstrap string addresses to SocketAddr
        let bootstrap_nodes = config.bootstrap_nodes.iter()
            .filter_map(|addr_str| {
                match addr_str.parse::<SocketAddr>() {
                    Ok(addr) => Some(addr),
                    Err(e) => {
                        // Try DNS resolution for hostnames
                        match dns_lookup(addr_str) {
                            Some(addr) => Some(addr),
                            None => {
                                eprintln!("Failed to resolve bootstrap node {}: {}", addr_str, e);
                                None
                            }
                        }
                    }
                }
            })
            .collect();
        
        let known_peers = Arc::new(Mutex::new(HashMap::new()));
        
        // Load saved peers
        if let Ok(peers) = load_peers_from_disk(&peer_file_path) {
            let mut known = known_peers.lock().unwrap();
            for peer in peers {
                known.insert(peer.id.clone(), peer);
            }
            println!("Loaded {} saved peers", known.len());
        }
        
        Ok(Node {
            id,
            listener,
            discovery_socket: Arc::new(discovery_socket),
            address,
            peers: Arc::new(Mutex::new(HashMap::new())),
            known_peers,
            bootstrap_nodes,
            message_cache: Arc::new(Mutex::new(HashSet::new())),
            max_hop_count: config.max_hop_count,
            running: Arc::new(Mutex::new(true)),
            peer_file_path,
            config,
        })
    }
    
    fn connect_to_peer(&self, peer_addr: SocketAddr) -> std::io::Result<bool> {
        let peer_id = format!("node_{}", peer_addr.to_string().replace(":", "_"));
        
        // Check if we're already connected to this peer
        {
            let peers = self.peers.lock().unwrap();
            if peers.contains_key(&peer_id) || peer_id == self.id {
                return Ok(false);
            }
        }
        
        println!("Attempting to connect to peer: {}", peer_addr);
        match TcpStream::connect_timeout(&peer_addr, Duration::from_secs(5)) {
            Ok(stream) => {
                stream.set_nonblocking(false)?;
                stream.set_read_timeout(Some(Duration::from_secs(30)))?;
                stream.set_write_timeout(Some(Duration::from_secs(5)))?;
                
                let mut peers = self.peers.lock().unwrap();
                peers.insert(peer_id.clone(), stream);
                
                // Update known peers
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                
                let mut known_peers = self.known_peers.lock().unwrap();
                known_peers.insert(peer_id.clone(), PeerInfo {
                    addr: peer_addr,
                    id: peer_id.clone(),
                    last_seen: now,
                });
                
                // Save updated peers list to disk
                let peers_list: Vec<PeerInfo> = known_peers.values().cloned().collect();
                let _ = save_peers_to_disk(&self.peer_file_path, &peers_list);
                
                println!("Connected to peer: {}", peer_addr);
                Ok(true)
            }
            Err(e) => {
                eprintln!("Failed to connect to {}: {}", peer_addr, e);
                Ok(false)
            }
        }
    }
    
    fn start(&self) {
        self.start_discovery();
        self.start_listening();
        
        // Initial peer discovery
        self.broadcast_discovery_packet();
        
        // Connect to bootstrap nodes
        for &addr in &self.bootstrap_nodes {
            let _ = self.connect_to_peer(addr);
        }
        
        // Connect to previously known peers
        let known_peers = self.known_peers.lock().unwrap().clone();
        for (_, peer_info) in known_peers {
            let _ = self.connect_to_peer(peer_info.addr);
        }
        
        // Start peer exchange thread
        self.start_peer_exchange();
        
        // Start peer list maintenance thread
        self.start_peer_maintenance();
    }
    
    fn start_discovery(&self) {
        let discovery_socket = Arc::clone(&self.discovery_socket);
        let peers = Arc::clone(&self.peers);
        let known_peers = Arc::clone(&self.known_peers);
        let node_id = self.id.clone();
        let running = Arc::clone(&self.running);
        let self_addr = self.address;
        let peer_file_path = self.peer_file_path.clone();
        
        // Thread for receiving discovery packets
        thread::spawn(move || {
            let mut buf = [0; 1024];
            
            while *running.lock().unwrap() {
                match discovery_socket.recv_from(&mut buf) {
                    Ok((size, src_addr)) => {
                        if src_addr.ip() == self_addr.ip() && src_addr.port() == self_addr.port() {
                            continue; // Skip self-messages
                        }
                        
                        let data = &buf[..size];
                        let msg_type = data[0];
                        
                        match msg_type {
                            // Discovery request
                            1 => {
                                let response = [2]; // PeerList message type
                                let _ = discovery_socket.send_to(&response, src_addr);
                                
                                // Try to connect to the discoverer
                                if let Some(port_bytes) = data.get(1..5) {
                                    if let Ok(port_array) = port_bytes.try_into() {
                                        let tcp_port = u32::from_be_bytes(port_array) as u16;
                                        let mut addr = src_addr;
                                        addr.set_port(tcp_port);
                                        
                                        if let Ok(stream) = TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
                                            let peer_id = format!("node_{}", addr.to_string().replace(":", "_"));
                                            let mut peers_map = peers.lock().unwrap();
                                            peers_map.insert(peer_id.clone(), stream);
                                            
                                            // Update known peers
                                            let now = std::time::SystemTime::now()
                                                .duration_since(std::time::UNIX_EPOCH)
                                                .unwrap()
                                                .as_secs();
                                            
                                            let mut known = known_peers.lock().unwrap();
                                            known.insert(peer_id.clone(), PeerInfo {
                                                addr,
                                                id: peer_id,
                                                last_seen: now,
                                            });
                                            
                                            // Save updated peers list
                                            let peers_list: Vec<PeerInfo> = known.values().cloned().collect();
                                            let _ = save_peers_to_disk(&peer_file_path, &peers_list);
                                        }
                                    }
                                }
                            }
                            // Peer list response
                            2 => {
                                let mut addr = src_addr;
                                
                                // Extract TCP port if provided
                                if data.len() >= 5 {
                                    if let Ok(port_array) = data[1..5].try_into() {
                                        let tcp_port = u32::from_be_bytes(port_array) as u16;
                                        addr.set_port(tcp_port);
                                        
                                        if let Ok(stream) = TcpStream::connect_timeout(&addr, Duration::from_secs(2)) {
                                            let peer_id = format!("node_{}", addr.to_string().replace(":", "_"));
                                            
                                            if peer_id != node_id {
                                                let mut peers_map = peers.lock().unwrap();
                                                peers_map.insert(peer_id.clone(), stream);
                                                
                                                // Update known peers
                                                let now = std::time::SystemTime::now()
                                                    .duration_since(std::time::UNIX_EPOCH)
                                                    .unwrap()
                                                    .as_secs();
                                                
                                                let mut known = known_peers.lock().unwrap();
                                                known.insert(peer_id.clone(), PeerInfo {
                                                    addr,
                                                    id: peer_id,
                                                    last_seen: now,
                                                });
                                                
                                                // Save updated peers list
                                                let peers_list: Vec<PeerInfo> = known.values().cloned().collect();
                                                let _ = save_peers_to_disk(&peer_file_path, &peers_list);
                                                
                                                println!("Added peer from discovery: {}", addr);
                                            }
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        if e.kind() != std::io::ErrorKind::WouldBlock {
                            eprintln!("Error receiving discovery packet: {}", e);
                        }
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        });
        
// Periodic discovery broadcast
let discovery_socket = Arc::clone(&self.discovery_socket);
let running = Arc::clone(&self.running);
let tcp_port = self.address.port();

thread::spawn(move || {
    while *running.lock().unwrap() {
        // Broadcast discovery packet every 30 seconds
        let mut discovery_packet = vec![1]; // 1 = Discovery request
        
        // Include our TCP port
        discovery_packet.extend_from_slice(&tcp_port.to_be_bytes());
        
        let broadcast_addr = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(255, 255, 255, 255)), 33445);
        let _ = discovery_socket.send_to(&discovery_packet, broadcast_addr);
        
        thread::sleep(Duration::from_secs(30));
    }
});
}

fn start_peer_exchange(&self) {
let known_peers = Arc::clone(&self.known_peers);
let peers = Arc::clone(&self.peers);
let running = Arc::clone(&self.running);
let node_id = self.id.clone();
let peer_file_path = self.peer_file_path.clone();
let exchange_interval = self.config.peer_exchange_interval;

// Peer exchange thread - periodically share known peers with connected peers
thread::spawn(move || {
    while *running.lock().unwrap() {
        thread::sleep(Duration::from_secs(exchange_interval));
        
        // Create a message with our known peers
        let known = known_peers.lock().unwrap();
        if known.is_empty() {
            continue;
        }
        
        // Only send peers that were seen in the last 24 hours
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        let recent_peers: Vec<PeerInfo> = known
            .values()
            .filter(|p| now - p.last_seen < 24 * 60 * 60) // Last 24 hours
            .cloned()
            .collect();
        
        if recent_peers.is_empty() {
            continue;
        }
        
        // Serialize the peer list
        let peers_json = serde_json::to_string(&recent_peers).unwrap_or_default();
        if peers_json.is_empty() {
            continue;
        }
        
        // Create a peer exchange message
        let message_id = format!("peerex_{}_{}", node_id, std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos());
        
        let mut seen_by = HashSet::new();
        seen_by.insert(node_id.clone());
        
        let content = format!("PEER_EXCHANGE:{}", peers_json).into_bytes();
        
        let message = Message {
            id: message_id,
            source: node_id.clone(),
            destination: None,
            content,
            hop_count: 0,
            seen_by,
        };
        
        // Send to all peers
        let peers_map = peers.lock().unwrap();
        for (_, stream) in peers_map.iter() {
            if let Ok(mut stream_clone) = stream.try_clone() {
                let _ = send_message(&mut stream_clone, &message);
            }
        }
    }
});
}

fn start_peer_maintenance(&self) {
let known_peers = Arc::clone(&self.known_peers);
let peers = Arc::clone(&self.peers);
let running = Arc::clone(&self.running);
let node_id = self.id.clone();
let peer_file_path = self.peer_file_path.clone();
let maintenance_interval = self.config.maintenance_interval;

// Maintenance thread - periodically clean up and reconnect
thread::spawn(move || {
    while *running.lock().unwrap() {
        thread::sleep(Duration::from_secs(maintenance_interval));
        
        // Get current time
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Update last_seen for currently connected peers
        {
            let peers_map = peers.lock().unwrap();
            let mut known = known_peers.lock().unwrap();
            
            for (peer_id, stream) in peers_map.iter() {
                if let Ok(addr) = stream.peer_addr() {
                    if let Some(peer_info) = known.get_mut(peer_id) {
                        peer_info.last_seen = now;
                    } else {
                        known.insert(peer_id.clone(), PeerInfo {
                            addr,
                            id: peer_id.clone(),
                            last_seen: now,
                        });
                    }
                }
            }
        }
        
        // Remove very old peers (older than 30 days)
        {
            let mut known = known_peers.lock().unwrap();
            known.retain(|_, info| now - info.last_seen < 30 * 24 * 60 * 60);
        }
        
        // Save peer list
        {
            let known = known_peers.lock().unwrap();
            let peers_list: Vec<PeerInfo> = known.values().cloned().collect();
            let _ = save_peers_to_disk(&peer_file_path, &peers_list);
        }
        
        // Try to connect to some known but not currently connected peers
        {
            let current_peers = peers.lock().unwrap();
            let known = known_peers.lock().unwrap();
            
            // Find peers that we know about but aren't connected to
            let candidates: Vec<PeerInfo> = known
                .values()
                .filter(|info| !current_peers.contains_key(&info.id) && info.id != node_id)
                .filter(|info| now - info.last_seen < 7 * 24 * 60 * 60) // Last week
                .cloned()
                .collect();
            
            // Release locks before attempting connections
            drop(current_peers);
            drop(known);
            
            // Try to connect to up to 5 random peers
            use rand::seq::SliceRandom;
            use rand::thread_rng;
            
            let mut rng = thread_rng();
            let sample = candidates
                .choose_multiple(&mut rng, 5.min(candidates.len()))
                .cloned()
                .collect::<Vec<_>>();
            
            for peer_info in sample {
                let addr = peer_info.addr;
                if let Ok(stream) = TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
                    let mut peers_map = peers.lock().unwrap();
                    if !peers_map.contains_key(&peer_info.id) {
                        peers_map.insert(peer_info.id.clone(), stream);
                        println!("Reconnected to known peer: {}", addr);
                    }
                }
            }
        }
    }
});
}

fn broadcast_discovery_packet(&self) {
let mut discovery_packet = vec![1]; // 1 = Discovery request
discovery_packet.extend_from_slice(&self.address.port().to_be_bytes());

let broadcast_addr = SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(255, 255, 255, 255)), 33445);
let _ = self.discovery_socket.send_to(&discovery_packet, broadcast_addr);
}

fn start_listening(&self) {
let peers = Arc::clone(&self.peers);
let message_cache = Arc::clone(&self.message_cache);
let known_peers = Arc::clone(&self.known_peers);
let node_id = self.id.clone();
let max_hop_count = self.max_hop_count;
let running = Arc::clone(&self.running);
let listener = self.listener.try_clone().unwrap();
let peer_file_path = self.peer_file_path.clone();

thread::spawn(move || {
    listener.set_nonblocking(true).unwrap();
    
    while *running.lock().unwrap() {
        match listener.accept() {
            Ok((stream, addr)) => {
                let peer_id = format!("node_{}", addr.to_string().replace(":", "_"));
                
                {
                    let mut peers_map = peers.lock().unwrap();
                    peers_map.insert(peer_id.clone(), stream.try_clone().unwrap());
                    
                    // Update known peers
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    
                    let mut known = known_peers.lock().unwrap();
                    known.insert(peer_id.clone(), PeerInfo {
                        addr,
                        id: peer_id.clone(),
                        last_seen: now,
                    });
                    
                    // Save peer list
                    let peers_list: Vec<PeerInfo> = known.values().cloned().collect();
                    let _ = save_peers_to_disk(&peer_file_path, &peers_list);
                }
                
                println!("Accepted connection from: {}", addr);
                
                let peers_clone = Arc::clone(&peers);
                let cache_clone = Arc::clone(&message_cache);
                let known_clone = Arc::clone(&known_peers);
                let node_id_clone = node_id.clone();
                let peer_file = peer_file_path.clone();
                
                thread::spawn(move || {
                    handle_connection(
                        peer_id,
                        stream,
                        peers_clone,
                        cache_clone,
                        known_clone,
                        node_id_clone,
                        max_hop_count,
                        peer_file,
                    );
                });
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::WouldBlock {
                    eprintln!("Error accepting connection: {}", e);
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
});
}

fn broadcast_message(&self, content: Vec<u8>, destination: Option<PeerId>) {
let message_id = format!("msg_{}_{}", self.id, std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap()
    .as_nanos());

let mut seen_by = HashSet::new();
seen_by.insert(self.id.clone());

let message = Message {
    id: message_id.clone(),
    source: self.id.clone(),
    destination,
    content,
    hop_count: 0,
    seen_by,
};

let mut cache = self.message_cache.lock().unwrap();
cache.insert(message_id);

let peers = self.peers.lock().unwrap();
for (_, stream) in peers.iter() {
    if let Ok(mut stream_clone) = stream.try_clone() {
        let _ = send_message(&mut stream_clone, &message);
    }
}
}

fn shutdown(&self) {
let mut running = self.running.lock().unwrap();
*running = false;
}
}

fn handle_connection(
peer_id: PeerId,
mut stream: TcpStream,
peers: Arc<Mutex<HashMap<PeerId, TcpStream>>>,
message_cache: Arc<Mutex<HashSet<String>>>,
known_peers: Arc<Mutex<HashMap<PeerId, PeerInfo>>>,
node_id: PeerId,
max_hop_count: u32,
peer_file_path: String,
) {
stream.set_nonblocking(true).unwrap();

let mut buffer = vec![0; 8192];

loop {
match stream.read(&mut buffer) {
    Ok(0) => {
        println!("Peer disconnected: {}", peer_id);
        let mut peers_map = peers.lock().unwrap();
        peers_map.remove(&peer_id);
        break;
    }
    Ok(size) => {
        let message = deserialize_message(&buffer[..size]);
        
        // Handle peer exchange messages specially
        if let Ok(content_str) = std::str::from_utf8(&message.content) {
            if content_str.starts_with("PEER_EXCHANGE:") {
                handle_peer_exchange(&content_str[13..], &known_peers, &peer_file_path);
                continue;
            }
        }
        
        let should_relay = {
            let mut cache = message_cache.lock().unwrap();
            if !cache.contains(&message.id) && message.hop_count < max_hop_count {
                cache.insert(message.id.clone());
                true
            } else {
                false
            }
        };
        
        if should_relay {
            let mut message = message;
            message.hop_count += 1;
            message.seen_by.insert(node_id.clone());
            
            match message.destination {
                Some(ref dest) if dest == &node_id => {
                    process_message(&message);
                }
                _ => {
                    let peers_map = peers.lock().unwrap();
                    for (id, peer_stream) in peers_map.iter() {
                        if !message.seen_by.contains(id) {
                            if let Ok(mut stream_clone) = peer_stream.try_clone() {
                                let _ = send_message(&mut stream_clone, &message);
                            }
                        }
                    }
                    
                    // If no destination specified or not yet reached destination, process locally too
                    if message.destination.is_none() || 
                       (message.destination.is_some() && message.destination.as_ref().unwrap() == &node_id) {
                        process_message(&message);
                    }
                }
            }
        }
    }
    Err(e) => {
        if e.kind() == std::io::ErrorKind::WouldBlock {
            thread::sleep(Duration::from_millis(10));
            continue;
        }
        
        eprintln!("Error reading from socket: {}", e);
        let mut peers_map = peers.lock().unwrap();
        peers_map.remove(&peer_id);
        break;
    }
}
}
}

fn handle_peer_exchange(
    json_data: &str,
    known_peers: &Arc<Mutex<HashMap<PeerId, PeerInfo>>>,
    peer_file_path: &str,
) {
    // Add debug logging
    println!("Received peer exchange data: length={}", json_data.len());
    if json_data.len() < 100 {
        println!("Data: {}", json_data);
    } else {
        println!("Data: {}...", &json_data[0..100]);
    }
    
    match serde_json::from_str::<Vec<PeerInfo>>(json_data) {
        Ok(peer_list) => {
            println!("Successfully parsed peer list with {} peers", peer_list.len());
            
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            
            let mut known = known_peers.lock().unwrap();
            let mut updated = false;
            
            for peer in peer_list {
                if !known.contains_key(&peer.id) || known[&peer.id].last_seen < peer.last_seen {
                    known.insert(peer.id.clone(), peer);
                    updated = true;
                }
            }
            
            if updated {
                // Save updated peer list
                let peers_list: Vec<PeerInfo> = known.values().cloned().collect();
                let _ = save_peers_to_disk(peer_file_path, &peers_list);
            }
        }
        Err(e) => {
            eprintln!("Failed to parse peer exchange data: {}", e);
            
            // Attempt to log a sample of the invalid data
            if !json_data.is_empty() {
                let sample = if json_data.len() > 200 {
                    &json_data[0..200]
                } else {
                    json_data
                };
                eprintln!("Invalid JSON data sample: {}", sample);
            } else {
                eprintln!("Received empty JSON data");
            }
        }
    }
}

fn send_message(stream: &mut TcpStream, message: &Message) -> std::io::Result<()> {
    let serialized = serialize_message(message);
    stream.write_all(&serialized)
}

fn serialize_message(message: &Message) -> Vec<u8> {
    serde_json::to_vec(message).unwrap_or_default()
}

fn deserialize_message(data: &[u8]) -> Message {
    serde_json::from_slice(data).unwrap_or_else(|_| {
        Message {
            id: String::new(),
            source: String::new(),
            destination: None,
            content: Vec::new(),
            hop_count: 0,
            seen_by: HashSet::new(),
        }
    })
}

fn process_message(message: &Message) {
    println!("Received message from {}: {:?}", message.source, String::from_utf8_lossy(&message.content));
}

// Helper function to resolve DNS names
fn dns_lookup(addr_str: &str) -> Option<SocketAddr> {
    if let Some(colon_pos) = addr_str.rfind(':') {
        let host = &addr_str[0..colon_pos];
        let port = &addr_str[colon_pos+1..];
        
        if let Ok(port_num) = port.parse::<u16>() {
            use std::net::ToSocketAddrs;
            if let Ok(addrs) = (host, port_num).to_socket_addrs() {
                return addrs.into_iter().next();
            }
        }
    }
    None
}

fn load_peers_from_disk(file_path: &str) -> std::io::Result<Vec<PeerInfo>> {
    let path = Path::new(file_path);
    if !path.exists() {
        return Ok(Vec::new());
    }
    
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    
    let mut peers = Vec::new();
    
    for line in reader.lines() {
        if let Ok(line) = line {
            if let Ok(peer_info) = serde_json::from_str(&line) {
                peers.push(peer_info);
            }
        }
    }
    
    Ok(peers)
}

fn save_peers_to_disk(file_path: &str, peers: &[PeerInfo]) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(file_path)?;
    
    let mut writer = std::io::BufWriter::new(file);
    
    for peer in peers {
        if let Ok(json) = serde_json::to_string(peer) {
            writeln!(writer, "{}", json)?;
        }
    }
    
    Ok(())
}

fn main() {
    // Use a config file path
    let config_path = "node_config.json";
    
    // Start a node with the config
    // Load config first to get the TCP port
    let config = load_config(config_path);
    let bind_addr = format!("0.0.0.0:{}", config.tcp_port);
    let node = Arc::new(Node::new(&bind_addr, config_path).unwrap());
    println!("Node started with ID: {} on address: {}", node.id, node.address);
    
    let node_for_start = Arc::clone(&node);
    node_for_start.start();
    
    // Example: Send a message after 5 seconds to give time for peer discovery
    let node_for_thread = Arc::clone(&node);
    
    thread::spawn(move || {
        thread::sleep(Duration::from_secs(5));
        node_for_thread.broadcast_message(b"Hello, decentralized world!".to_vec(), None);
        println!("Broadcast message sent from {}", node_for_thread.id);
    });
    
    // Keep the main thread alive and handle user input
    let mut input = String::new();
    println!("Enter messages to broadcast (or 'quit' to exit):");
    println!("Commands:");
    println!("  direct:<node_id>:<message> - Send a direct message to a specific node");
    println!("  connect:<ip>:<port> - Connect to a specific peer");
    println!("  peers - List connected peers");
    println!("  known - List all known peers");
    println!("  discover - Send a discovery broadcast");
    println!("  bootstrap - Connect to bootstrap nodes");
    println!("  config - Display current configuration");
    
    loop {
        input.clear();
        std::io::stdin().read_line(&mut input).unwrap();
        
        let input = input.trim();
        
        if input == "quit" {
            break;
        } else if input.starts_with("direct:") {
            // Format: direct:node_id:message
            let parts: Vec<&str> = input.splitn(3, ':').collect();
            if parts.len() == 3 {
                let target = parts[1].to_string();
                let message = parts[2].as_bytes().to_vec();
                node.broadcast_message(message, Some(target.clone()));
                println!("Direct message sent to {}", target);
            } else {
                println!("Invalid format. Use: direct:node_id:message");
            }
        } else if input.starts_with("connect:") {
            // Format: connect:ip:port
            let parts: Vec<&str> = input.splitn(3, ':').collect();
            if parts.len() == 3 {
                if let (Ok(ip), Ok(port)) = (parts[1].parse::<IpAddr>(), parts[2].parse::<u16>()) {
                    let addr = SocketAddr::new(ip, port);
                    match node.connect_to_peer(addr) {
                        Ok(true) => println!("Connected to {}", addr),
                        Ok(false) => println!("Already connected or connection failed"),
                        Err(e) => println!("Connection error: {}", e),
                    }
                } else {
                    println!("Invalid IP or port");
                }
            } else {
                println!("Invalid format. Use: connect:ip:port");
            }
        } else if input == "peers" {
            let peers = node.peers.lock().unwrap();
            println!("Connected peers ({}): ", peers.len());
            for (id, _) in peers.iter() {
                println!("  - {}", id);
            }
        } else if input == "known" {
            let known = node.known_peers.lock().unwrap();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
                
            println!("Known peers ({}): ", known.len());
            for (id, info) in known.iter() {
                let age = now - info.last_seen;
                let age_str = if age < 60 {
                    format!("{} seconds ago", age)
                } else if age < 3600 {
                    format!("{} minutes ago", age / 60)
                } else if age < 86400 {
                    format!("{} hours ago", age / 3600)
                } else {
                    format!("{} days ago", age / 86400)
                };
                
                println!("  - {} at {} (last seen: {})", id, info.addr, age_str);
            }
        } else if input == "discover" {
            println!("Sending discovery broadcast...");
            node.broadcast_discovery_packet();
        } else if input == "bootstrap" {
            println!("Connecting to bootstrap nodes...");
            for &addr in &node.bootstrap_nodes {
                match node.connect_to_peer(addr) {
                    Ok(true) => println!("Connected to bootstrap node {}", addr),
                    Ok(false) => println!("Already connected or connection failed to {}", addr),
                    Err(e) => println!("Error connecting to bootstrap node {}: {}", addr, e),
                }
            }
        } else if input == "config" {
            println!("Current configuration:");
            println!("  Discovery port: {}", node.config.discovery_port);
            println!("  Max hop count: {}", node.config.max_hop_count);
            println!("  Peer exchange interval: {} seconds", node.config.peer_exchange_interval);
            println!("  Maintenance interval: {} seconds", node.config.maintenance_interval);
            println!("  Bootstrap nodes:");
            for addr in &node.config.bootstrap_nodes {
                println!("    - {}", addr);
            }
        } else if !input.is_empty() {
            // Broadcast a regular message
            node.broadcast_message(input.as_bytes().to_vec(), None);
            println!("Broadcast message sent");
        }
    }
    
    println!("Shutting down node...");
    node.shutdown();
    thread::sleep(Duration::from_secs(1));
}
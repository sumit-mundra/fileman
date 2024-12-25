use dbscan::{Classification, Model};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Instant, UNIX_EPOCH};
use std::{fs, io};

fn main() {
    let start = Instant::now();
    println!("{:?}", start);
    let input_path = "/Users/sumitmundra/Desktop/oldDSLRNikonShots";
    let entries = list_paths(&input_path);
    let clusters = build_clusters(entries.clone());
    println!("{:?}", clusters);
    println!("{:?}", start.elapsed());
}

fn build_clusters<'a>(entries: Vec<PathBuf>) -> HashMap<usize, Vec<String>> {
    let file_db = build_model_input(&entries.clone());
    let model = Model::new(3600.0, 3);
    let output = model.run(&file_db);
    let mut clusters: HashMap<usize, Vec<String>> = HashMap::new();
    for (entry, classification) in entries.iter().zip(output.iter()) {
        let path = entry.as_path().to_str().unwrap().to_string();
        match classification {
            Classification::Core(cluster_id) => {
                let data_vec = clusters.entry(*cluster_id).or_insert(Vec::new());
                data_vec.push(path);
            }
            Classification::Edge(cluster_id) => {
                let data_vec = clusters.entry(*cluster_id).or_insert(Vec::new());
                data_vec.push(path);
            }
            Classification::Noise => {}
        }
    }
    clusters
}

fn build_model_input(entries: &Vec<PathBuf>) -> Vec<Vec<f64>> {
    let mut file_db: Vec<Vec<f64>> = Vec::new();
    for entry in entries {
        if let Ok(metadata) = entry.metadata() {
            if let Ok(created) = metadata.created() {
                if let Ok(create_duration) = created.duration_since(UNIX_EPOCH) {
                    file_db.push(vec![create_duration.as_secs() as f64, 0.0]);
                }
            }
        } else {
            println!("Couldn't get metadata for {:?}", entry.as_path());
        }
    }
    file_db
}

fn list_paths(input_path: &&str) -> Vec<PathBuf> {
    let mut entries = fs::read_dir(&input_path).unwrap()
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<PathBuf>, io::Error>>().unwrap();
    entries
}

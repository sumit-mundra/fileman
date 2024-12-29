use clap::Parser;
use dbscan::{Classification, Model};
use fs::remove_dir_all;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Instant, UNIX_EPOCH};
use std::{fs, io};

/// fileman is a simple tool to group files based on create_date os timestamp using dbscan algorithm.
/// author: Sumit Mundra
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// the directory to scan
    #[arg(short, long)]
    input_path: PathBuf,
    /// output directory with links to input files grouped inside cluster-wise folders
    #[arg(short = 'o', long)]
    target_path: PathBuf,
    /// duration in seconds between two files to cluster them together
    #[arg(short, long, default_value_t = 600.0)]
    time_interval_sec: f64,
    /// minimum count of files needed to define a cluster
    #[arg(short = 'c', long, default_value_t = 3)]
    min_cluster_size: usize,
    /// prefix to be used as tag and as output directory prefix, no tagging if empty
    #[arg(short = 'p', long, default_value = "cluster")]
    tag_prefix: String,
}
fn main() {
    let args = Args::parse();
    let start = Instant::now();
    let entries = list_paths(&args.input_path);
    let clusters = build_clusters(entries.clone(), args.time_interval_sec, args.min_cluster_size);
    create_symlinks(&clusters, &args.target_path, &args.tag_prefix);
    prune_old_tags(entries);
    if !String::is_empty(&args.tag_prefix) {
        create_macos_tags(&clusters, &args.tag_prefix);
    }
    println!("Finished in {:?} ms", start.elapsed().as_millis());
}

fn prune_old_tags(entries: Vec<PathBuf>) {
    for entry in entries {
        macos_tags::prune_tags(&entry).ok();
    }
}

fn create_macos_tags(clusters: &HashMap<usize, Vec<String>>, prefix: &str) {
    for (cluster_id, list) in clusters {
        for item in list {
            let path = Path::new(item);
            let t = macos_tags::Tag::Custom(format!("{prefix}_{cluster_id}"));
            macos_tags::add_tag(path, &t).expect("Could not create tag");
        }
    }
}

fn build_clusters<'a>(entries: Vec<PathBuf>, time_interval_sec: f64, cluster_size: usize) -> HashMap<usize, Vec<String>> {
    let file_db = build_model_input(&entries.clone());
    let model = Model::new(time_interval_sec, cluster_size);
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

fn create_symlinks(clusters: &HashMap<usize, Vec<String>>, out_path_buf: &PathBuf, prefix: &str) {
    let output_path = fs::exists(&out_path_buf).expect("Failed to check for output path existing or not");
    let path = out_path_buf.as_path().to_str().expect("Could not convert output path buffer to path");
    if output_path {
        remove_dir_all(path).expect("Could not delete directory recursively");
    }
    fs::create_dir(path).expect("Could not create directory");
    // create a directory for each cluster and create symlink within
    for (cluster_id, list) in clusters {
        let mut pb = PathBuf::from(&path);
        pb.push(format!("{prefix}_{cluster_id}"));
        fs::create_dir_all(pb.as_path()).expect("Could not create directory");
        for item in list {
            let file_name = Path::new(&item).file_name().unwrap().to_str().unwrap();
            let mut link_path = pb.clone();
            link_path.push(file_name);
            std::os::unix::fs::symlink(item, link_path).expect("Failed to create symlink");
            // std::os::windows::fs::symlink_file(item, link_path).expect("Failed to create symlink");
        }
    }
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

fn list_paths(input_path: &PathBuf) -> Vec<PathBuf> {
    let entries = fs::read_dir(&input_path).unwrap()
        .map(|res| res.map(|e| fs::canonicalize(e.path()).unwrap()))
        .collect::<Result<Vec<PathBuf>, io::Error>>().unwrap();
    entries
}

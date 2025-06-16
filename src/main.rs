use opendal::layers::LoggingLayer;
use opendal::services::{Fs, Postgresql, S3};
use opendal::Operator;
use opendal::Result;

// Process of using OpenDAL:
// 1. init service
// 2. compose layers
// 3. user operator

async fn scenario_fs() -> Result<()> {
    // service: POSIX file system
    // supports: stat, read, write, append, create_dir, delete, copy, rename, list, blocking
    // doc: https://docs.rs/opendal/latest/opendal/services/struct.Fs.html
    println!("# 1. Scenario FS");
    let builder = Fs::default()
        .root("/tmp");
    let op: Operator = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish();

    let path = "hello.txt";
    let bs = "Hello, World!";
    op.write(path, bs).await?;
    println!("> (1) write: \"{}\" to {}", bs, path);

    let bs = op.read(path).await?;
    println!("> (2) read: \"{}\"", String::from_utf8(bs.to_vec()).unwrap());

    let meta = op.stat(path).await?;
    println!("> (3) stat: {:?}", meta);

    op.delete(path).await?;
    println!();
    Ok(())
}

async fn scenario_postgres() -> Result<()> {
    // service: PostgreSQL
    // supports: stat, read, write, create_dir, delete
    // doc: https://docs.rs/opendal/latest/opendal/services/struct.Postgresql.html
    println!("# 2. Scenario PostreSQL");
    let mut builder = Postgresql::default()
        .root("/")
        .connection_string("postgresql://jakejs:password@127.0.0.1:5432/default_database")
        .table("data")
        .key_field("key")
        .value_field("value");
    let op: Operator = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish();
    
    let path = "hello.txt";
    let bs = "Hello, World!";
    op.write(path, bs).await?;
    println!("> (1) write: \"{}\" to {}", bs, path);

    let bs = op.read(path).await?;
    println!("> (2) read: \"{}\"", String::from_utf8(bs.to_vec()).unwrap());

    let meta = op.stat(path).await?;
    println!("> (3) stat: {:?}", meta);
    
    op.delete(path).await?;
    println!();
    Ok(())
}

async fn scenario_s3() -> Result<()> {
    // service: S3
    // supports: stat, read, write, append, create_dir, delete, copy, list, presign
    // doc: https://docs.rs/opendal/latest/opendal/services/struct.S3.html
    println!("# 3. Scenario S3");
    let mut builder = S3::default()
        .root("/")
        .bucket("jakejstestbucket")
        .region("ap-northeast-2")
        // .endpoint() // Default to "https://s3.amazonaws.com"
        .access_key_id("<your_access_key_id>")
        .secret_access_key("<your_secret_access_key>");
    let op: Operator = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .finish();

    let path = "hello.txt";
    let bs = "Hello, World!";
    op.write(path, bs).await?;
    println!("> (1) write: \"{}\" to {}", bs, path);

    let bs = op.read(path).await?;
    println!("> (2) read: \"{}\"", String::from_utf8(bs.to_vec()).unwrap());

    let meta = op.stat(path).await?;
    println!("> (3) stat: {:?}", meta);
    
    op.delete(path).await?;
    println!();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    scenario_fs().await?;
    scenario_postgres().await?;
    scenario_s3().await?;
    Ok(())
}
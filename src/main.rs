use chrono::prelude::*;
use std::io::prelude::*;
use std::{
    process::{Command,Stdio},
    io::{BufReader},
    collections::{HashMap},
    thread,
    time,
    fs,
    fs::File,
    env,
    env::VarError::{NotUnicode,NotPresent},
};
use reqwest::{StatusCode};

struct FileStruct {
        file_date: NaiveDateTime,
        file_name: String,
}

fn remove_file(file_name: &str,aws_url: &str) -> Result<(),String> {
    let rm = Command::new("aws")
    .arg("s3")
    .arg("rm")
    .arg(format!("{}/{}",aws_url,file_name))
    .output()
    .map_err(|e| format!("Error spawning process to remove file from AWS:{}",e.to_string()))?;

    if !rm.status.success() {
        log::error!("aws rm command failed");
        return Err("aws rm command failed".to_string());
    }

    log::debug!("Removed {} from aws",file_name);
    //display any output from aws copy command
    //io::stderr().write_all(&rm.stderr).unwrap();   
    //io::stdout().write_all(&rm.stdout).unwrap();
    Ok(())
}

fn get_filename() -> String {
    let local: DateTime<Local> = Local::now();
    format!("mysql-{}-{:02}-{:02}:{:02}:{:02}:{:02}.gz",local.year(),local.month(),local.day(),local.hour(),local.minute(),local.second())
}

fn create_temp_file(file_name: &str) -> Result<File,String> {
    File::create(&file_name).map_err(|e| format!("Error opening file:{}:",e.to_string()))
}

fn do_backup(temp_file: File, user: &str,mysql_host: &str) -> Result<(),String> {
    log::debug!("mysqldump started");
    let mut backup = Command::new("mysqldump")
    .arg("-y")
    .arg("-h")
    //.arg("127.0.0.1")
    .arg(mysql_host)
    .arg("-u")
    .arg(user)
    .arg("-A")
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .map_err(|e| format!("Error spawning mysqldump:{}",e.to_string()))?;


        

    if let Some(backup_output) = backup.stdout.take() {
        let compress = Command::new("gzip")
            .stdout(Stdio::from(temp_file))
            .stdin(backup_output)
            .status()
            .map_err(|e| format!("Error spawning gzip:{}",e.to_string()))?;

        if !compress.success() {
            log::error!("gzip command failed");
            return Err("gzip command failed".to_string());
        }
    }

    if let Some(ref mut backup_err) = backup.stderr {
        let mut reader = BufReader::new(backup_err);

        let mut line = String::new();
        let len = reader.read_line(&mut line).map_err(|e| format!("error reading stderr:{}
",e.to_string()))?;
        if len > 0 {
            log::error!("{}",line);
        }
    }

    let backup_status = backup.wait().map_err(|e| format!("Error spawning mysqldump:{}",e.to_string()))?;

    if !backup_status.success() {
        return Err("mysqldump command failed".to_string());
    }

    log::debug!("mysqldump finished");
    Ok(())
}

fn cp_temp_to_aws(file_name: &str, aws_url: &str) -> Result<(),String> {
    let cp = Command::new("aws")
    .arg("s3")
    .arg("cp")
    .arg(&file_name)
    .arg(aws_url)
    .output()
    .map_err(|e| format!("Error spawning process to copy file to AWS:{}",e.to_string()))?;

    if !cp.status.success() {
        log::error!("cp command failed");
        return Err("aws copy command failed".to_string());
    }

    log::debug!("Uploaded {} to aws",file_name);

    Ok(())
}

fn remove_stale_files_from_aws(aws_url: &str,backups_to_keep: usize) -> Result<(),String> {
    let output = Command::new("aws")
    .arg("s3")
    .arg("ls")
    .arg(format!("{}/mysql",aws_url))
    .output()
    .map_err(|e| format!("Error spawning aws list process:{}",e.to_string()))?;

    if !output.status.success() {
        log::error!("command failed");
        return Err("aws ls commmand failed".to_string());
    }

    let ls_output = String::from_utf8(output.stdout).map_err(|e| format!("ls conversion to utf8 failed:{}",e.to_string()))?;
    let mut backup_files: Vec<_> = ls_output
        .lines()
        .filter_map(|l| {
            let fields: Vec<&str> = l.split_whitespace().collect();
            let d = match NaiveDate::parse_from_str(fields[0],"%Y-%m-%d") {
                Err(e) => {
                    log::error!("Error parsing date: {}",e.to_string());
                    return None;
                },
                Ok(o) => o
            };
            let t = match NaiveTime::parse_from_str(fields[1],"%H:%M:%S") {
                Err(e) => {
                    log::error!("Error parsing time: {}",e.to_string());
                    return None;
                },
                Ok(o) => o
            };
            let dt = NaiveDateTime::new(d,t);
            let file = FileStruct {
                file_date: dt,
                file_name: fields[3].to_string(),
            };

            Some(file)
        })
        .collect();

    backup_files.sort_by(|a,b| a.file_date.partial_cmp(&b.file_date).unwrap());

    backup_files
        .iter() 
        .rev()
        .skip(backups_to_keep)
        .for_each(|f| {
            if let Err(e) = remove_file(&f.file_name,&aws_url) {
                log::error!("Error removing file from aws:{}",e.to_string());
            }
        });
    Ok(())
}

fn send_to_slack(file_name: &str,slack_url: &str) -> Result<(),String> {
    let mut data = HashMap::new();
    data.insert("text", format!("database backed up: {}",file_name));

    let client = reqwest::blocking::Client::new();
    let res = client.post(slack_url)
        .json(&data)
        .send()
        .map_err(|e| format!("Could not send to slack:{}",e.to_string()))?;

    if res.status() != StatusCode::OK {
        return Err(format!("slack call returned: {}",res.status().to_string()));
    }
    Ok(())
}

fn main_loop(mysql_user: &str, mysql_host: &str, aws_url: &str,backups_to_keep: usize,slack_url: &Option<String>) -> Result<(),String> {
    let file_name = get_filename();

    let temp_file = create_temp_file(&file_name)?;

    do_backup(temp_file,&mysql_user,&mysql_host)?;
    
    cp_temp_to_aws(&file_name,&aws_url)?;
    
    remove_stale_files_from_aws(&aws_url,backups_to_keep)?;

    //remove the temporary file
    fs::remove_file(&file_name).map_err(|e| format!("error removing temporary file:{}",e.to_string()))?;

    if let Some(su) = slack_url {
        send_to_slack(&file_name, &su)?;
    }

    Ok(())
}

fn main() -> Result<(),String> {
    env_logger::init();

    let sleep_duration = env::var("SLEEP_DURATION").unwrap_or("86400".to_string());
    let sleep_duration: u64 = sleep_duration.parse().map_err(|_| format!("SLEEP_DURATION environment variable is not an integer"))?;
    log::info!("Backup will be performed once every {} seconds.",sleep_duration);
    let sleep_duration = time::Duration::from_secs(sleep_duration);

    let backups_to_keep = env::var("BACKUPS_TO_KEEP").unwrap_or("3".to_string());
    let backups_to_keep: usize = backups_to_keep.parse().map_err(|_| format!("BACKUPS_TO_KEEP environment variable is not an integer"))?;
    log::info!("{} backups will be kept",backups_to_keep);

    if env::var("MYSQL_PWD").is_err() {
        return Err("MYSQL_PWD is not set".to_string())
    }

    let mysql_user = env::var("MYSQL_USER").map_err(|_| "MYSQL_USER is not set".to_string())?;
    env::var("AWS_ACCESS_KEY_ID").map_err(|_| "AWS_ACCESS_KEY_ID is not set".to_string())?;
    env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| "AWS_SECRET_ACCESS_KEY is not set".to_string())?;
    env::var("AWS_DEFAULT_REGION").map_err(|_| "AWS_DEFAULT_REGION is not set".to_string())?;

    let aws_url = env::var("AWS_URL").map_err(|_| "AWS_URL is not set".to_string())?;
    let mysql_host = env::var("MYSQL_HOST").map_err(|_| "MYSQL_HOST is not set".to_string())?;
    let slack_url = match env::var("SLACK_URL") {
        Ok(s) => Some(s),
        Err(NotPresent) => {
            log::info!("SLACK_URL is not set");
            None
        },
        Err(NotUnicode(_)) => return Err(format!("SLACK_URL env failed"))
    };

    loop {
        match main_loop(&mysql_user,&mysql_host, &aws_url,backups_to_keep,&slack_url) {
            Err(e) => {
                log::error!("{}",e);
                log::debug!("Sleeping for 60 seconds.");
                //wait for one minute then retry
                let sleep_duration = time::Duration::from_secs(60);
                thread::sleep(sleep_duration);
                continue;
            },
            Ok(o) => o
        }
        log::debug!("Sleeping for {} seconds.",sleep_duration.as_secs());
        thread::sleep(sleep_duration);
    }
}

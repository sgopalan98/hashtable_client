use std::io::prelude::*;
use std::time::{Instant, Duration};
use std::thread;
use std::net::TcpStream;
use std::io::BufReader;

fn main() {
    let now = Instant::now();
    let server = "0.0.0.0:7878";
    let no_of_threads = 100;

    let mut put_threads = vec![];
    for i in 0..no_of_threads {
        put_threads.push(thread::spawn(move || {
            let sleep_millis = no_of_threads - i;
            thread::sleep(Duration::from_millis(sleep_millis));
            let key = i;
            let value = i;
            let command = format!("PUT {} {}\n", key, value);

            //Send command
            let mut stream = TcpStream::connect(server).unwrap();
            stream.write(command.as_bytes()).unwrap();
            
            let mut reader = BufReader::new(&stream);
            let mut error_code = String::new();
            reader.read_line(&mut error_code).unwrap();

            // Check error code
            if error_code.trim().parse::<i32>().unwrap() == 0 {
                let mut value = String::new();
                reader.read_line(&mut value).unwrap();
            }
            else {
                let mut error_value = String::new();
                reader.read_line(&mut error_value).unwrap();
                error_value.trim().parse::<i32>().unwrap();
            }
        }));
    }
    
    for thread in put_threads {
        // Wait for the thread to finish. Returns a result.
        let _ = thread.join();
    }
    let elapsed = now.elapsed();
    println!("PUT elapsed: {:.2?}", elapsed);

    thread::sleep(Duration::from_secs(2));

    let mut get_threads = vec![];
    for i in 0..no_of_threads {
        get_threads.push(thread::spawn(move || {
            let sleep_millis = no_of_threads - i;
            thread::sleep(Duration::from_millis(sleep_millis));
            let key = i;
            let command = format!("GET {}\n", key);
            //Send command
            let mut stream = TcpStream::connect(server).unwrap();
            stream.write(command.as_bytes()).unwrap();
            
            let mut reader = BufReader::new(&stream);
            let mut error_code = String::new();
            reader.read_line(&mut error_code).unwrap();
            // Check error code
            if error_code.trim().parse::<i32>().unwrap() == 0 {
                let mut value = String::new();
                reader.read_line(&mut value).unwrap();
                assert!(value.trim().parse::<i32>().unwrap() == i.try_into().unwrap(), "value = {} i = {}", value.trim().parse::<i32>().unwrap(), i);
            }
            else {
                let mut error_value = String::new();
                reader.read_line(&mut error_value).unwrap();
                error_value.trim().parse::<i32>().unwrap();
            }
        }));
    }

    for thread in get_threads {
        // Wait for the thread to finish. Returns a result.
        let _ = thread.join();
    }

    let elapsed = now.elapsed();
    println!("Total Elapsed: {:.2?}", elapsed);
}

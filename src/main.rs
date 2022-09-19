use std::io::{prelude::*, BufWriter};
use std::time::{Instant, Duration};
use std::{thread, error, fs};
use std::net::TcpStream;
use std::io::BufReader;

fn evaluate_hashtable(server: &'static str, no_of_threads: usize, no_of_items: usize, get_per_put: i32) -> Duration {
    let items_per_thread = no_of_items / no_of_threads;
    let iterations = 10;
    let items_per_iteration = items_per_thread / iterations;
    let mut threads = vec![];
    let now = Instant::now();
    for thread_no in 0..no_of_threads {
        threads.push(thread::spawn(move || {
            let stream = loop{
                let error_code = TcpStream::connect(server);
                match error_code {
                  Ok(stream) => break stream,
                  Err(_) => {
                    println!("Connection Failed");
                    continue;
                  },
                }
            };
            let stream_clone = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            let mut writer = BufWriter::new(stream_clone);
            for iteration in 0..iterations {

                let base = thread_no * items_per_thread + iteration * items_per_iteration;
                let end = base + items_per_iteration;

                // PUT
                for key in base..end {
                    let command = format!("PUT {} {}\n", key, key);
                    writer.write(command.as_bytes()).unwrap();
                    writer.flush().unwrap();

                    let mut error_code = String::new();
                    reader.read_line(&mut error_code).unwrap();
                    if error_code.trim().parse::<i32>().unwrap() == 0 {
                        let mut value = String::new();
                        reader.read_line(&mut value).unwrap();
                    }
                    else {
                        let mut error_value = String::new();
                        reader.read_line(&mut error_value).unwrap();
                        println!("FAILED DUE TO {}\n", error_value);
                    }
                }

                
                // GET
                for get_index in 0..get_per_put {
                    for key in base..end {
                        let command = format!("GET {}\n", key);
                        writer.write(command.as_bytes()).unwrap();
                        writer.flush().unwrap();
                        let mut error_code = String::new();
                        reader.read_line(&mut error_code).unwrap();
                        if error_code.trim().parse::<i32>().unwrap() == 0 {
                            let mut value = String::new();
                            reader.read_line(&mut value).unwrap();
                            assert!(value.trim().parse::<i32>().unwrap() == key.try_into().unwrap(), "value = {} actual = {}", value.trim().parse::<i32>().unwrap(), key);
                        }
                        else {
                            let mut error_value = String::new();
                            reader.read_line(&mut error_value).unwrap();
                            println!("FAILED DUE TO {}", error_value);
                            error_value.trim().parse::<i32>().unwrap();
                        }
                    }
                }
                println!("For thread {} iteration {} completed\n", thread_no, iteration);
            }
            let command = format!("CLOSE");
            writer.write(command.as_bytes()).unwrap();
            writer.flush().unwrap();
        }));
    }
    
    for thread in threads {
        // Wait for the thread to finish. Returns a result.
        let _ = thread.join();
    }

    let elapsed = now.elapsed();
    return elapsed;
}

fn main() {
    let server: &'static str = "0.0.0.0:7878";
    let no_of_threads = 4; // No of hyperthreads
    let no_of_items: usize = 100000;
    let mut elapsed_duration = vec![];
    let get_per_puts: Vec<i32> = (1..=10).collect();
    for get_per_put in get_per_puts {
        elapsed_duration.push(evaluate_hashtable(server, no_of_threads, no_of_items, get_per_put));
    }
    let mut file_output:String = "".to_owned();
    for (index, duration) in elapsed_duration.iter().enumerate() {
        let no_of_operations = no_of_items + no_of_items * (index + 1);
        println!("TIME TAKEN {:?}", duration.as_micros());
        let throughput = no_of_operations * i32::pow(10, 6) as usize / duration.as_micros() as usize;
        println!("THROUGHPUT {}", throughput);
        let output_string = format!("{},{:?}\n", index + 1, throughput);
        file_output.push_str(&output_string);
    }
    fs::write("output.txt", file_output).unwrap();
}

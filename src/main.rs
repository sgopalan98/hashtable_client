use std::io::prelude::*;
use std::time::{Instant, Duration};
use std::{thread, error};
use std::net::TcpStream;
use std::io::BufReader;

fn main() {
    let now = Instant::now();
    let server = "0.0.0.0:7878";
    let no_of_threads = 2; // No of hyperthreads
    let no_of_items = 50000;
    let items_per_thread = no_of_items / no_of_threads;
    let iterations = 10;
    let items_per_iteration = items_per_thread / iterations;
    let get_per_put = 10;
    let mut threads = vec![];
    for thread_no in 0..no_of_threads {
        threads.push(thread::spawn(move || {


            
            for iteration in 0..iterations {

                let base = thread_no * items_per_thread + iteration * items_per_iteration;
                let end = base + items_per_iteration;

                // PUT
                for key in base..end {
                    let mut stream = loop{
                        let error_code = TcpStream::connect(server);
                        match error_code {
                          Ok(stream) => break stream,
                          Err(_) => {
                            println!("Connection Failed");
                            continue;
                          },
                        }
                    };
                    let command = format!("PUT {} {}\n", key, key);
                    stream.write(command.as_bytes()).unwrap();


                    let mut reader = BufReader::new(&stream);
                    let mut error_code = String::new();
                    reader.read_line(&mut error_code).unwrap();
                }

                println!("For thread {} PUT for iteration {} completed\n", thread_no, iteration + 1);
                
                // GET
                for get_index in 0..get_per_put {
                    for key in base..end {
                        let mut stream = loop{
                            let error_code = TcpStream::connect(server);
                            match error_code {
                              Ok(stream) => break stream,
                              Err(_) => {
                                println!("Connection Failed");
                                continue;
                              },
                            }
                        };
                        let command = format!("GET {}\n", key);
                        stream.write(command.as_bytes()).unwrap();


                        let mut reader = BufReader::new(&stream);
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
                            error_value.trim().parse::<i32>().unwrap();
                        }
                    }
                    println!("For thread {} get iteration {} completed\n", thread_no, get_index);
                }
                println!("For thread {} iteration {} completed\n", thread_no, iteration);
            }
        }));
    }
    
    for thread in threads {
        // Wait for the thread to finish. Returns a result.
        let _ = thread.join();
    }

    let elapsed = now.elapsed();
    println!("Total elapsed: {:.2?}", elapsed);
}

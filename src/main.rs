use plotters::prelude::*;
use std::io::{prelude::*, BufWriter};
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::{thread, fs};
use std::net::TcpStream;
use std::io::BufReader;
use rand::seq::SliceRandom;

fn evaluate_hashtable(server: &'static str, no_of_threads: usize, input_vector: Vec<usize>, get_per_put: i32) -> Duration {
    let iterations = 10;
    let no_of_items = input_vector.len();
    let items_per_thread = no_of_items / no_of_threads;
    let items_per_iteration = items_per_thread / iterations;
    let mut threads = vec![];
    let now = Instant::now();
    let arc_input = Arc::new(input_vector);
    for thread_no in 0..no_of_threads {
        let input = Arc::clone(&arc_input); 
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
                let thread_input = &input[base..end]; 
                // PUT
                for key in thread_input {
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
                for _ in 0..get_per_put {
                    for key in thread_input {
                        let command = format!("GET {}\n", key);
                        writer.write(command.as_bytes()).unwrap();
                        writer.flush().unwrap();
                        let mut error_code = String::new();
                        reader.read_line(&mut error_code).unwrap();
                        if error_code.trim().parse::<i32>().unwrap() == 0 {
                            let mut value = String::new();
                            reader.read_line(&mut value).unwrap();
                            assert!(value.trim().parse::<usize>().unwrap() == *key, "value = {} actual = {}", value.trim().parse::<i32>().unwrap(), key);
                        }
                        else {
                            let mut error_value = String::new();
                            reader.read_line(&mut error_value).unwrap();
                            println!("FAILED DUE TO {}", error_value);
                            error_value.trim().parse::<i32>().unwrap();
                        }
                    }
                }
            }
            let command = format!("CLOSE");
            writer.write(command.as_bytes()).unwrap();
            writer.flush().unwrap();
            println!("GET/PUT {} done!", get_per_put);
        }));
    }
    
    for thread in threads {
        // Wait for the thread to finish. Returns a result.
        let _ = thread.join();
    }

    let elapsed = now.elapsed();
    let t = thread::spawn(move || {

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
        let command = "RESET 0";
        writer.write(command.as_bytes()).unwrap();
        writer.flush().unwrap();
    });

    t.join().unwrap();
    return elapsed;
}


fn generate_throughput_graph(data: Vec<(f64, f64)>, label: &str, path: &str) {
    // Drawing area
    let root = BitMapBackend::new(path, (640, 480)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    let mut chart = ChartBuilder::on(&root)
        .caption(label, ("sans-serif", 50).into_font())
        .margin(5)
        .x_label_area_size(30)
        .y_label_area_size(30)
        .build_cartesian_2d(0.0..100.0, 0.0..1.0).unwrap();

    chart.configure_mesh().draw().unwrap();
    
    // Drawing line
    chart
        .draw_series(LineSeries::new(data.iter().map(|pair| (pair.0 as f64, pair.1 as f64)), BLUE.filled()).point_size(4)).unwrap();

    chart
        .configure_series_labels()
        .background_style(&WHITE.mix(0.8))
        .border_style(&BLACK)
        .draw().unwrap();

    root.present().unwrap();
}

fn main() {
    let single_lock_server: &'static str = "0.0.0.0:7878";
    let striped_lock_server: &'static str = "0.0.0.0:7879";
    let no_of_threads = 4; // No of hyperthreads
    let no_of_items: usize = 100000;
    let mut elapsed_duration = vec![];
    
    // Input
    let base = 0;
    let end = no_of_items - 1;
    let mut input: Vec<_> = (base..=end).collect();
    let mut rng = rand::thread_rng();
    input.shuffle(&mut rng);

    // Single lock server
    let get_per_puts: Vec<i32> = (1..=5).collect();
    for get_per_put in get_per_puts.clone() {
        elapsed_duration.push(evaluate_hashtable(single_lock_server, no_of_threads, input.clone(), get_per_put));
    }
    let mut throughput_values = vec![];
    for (index, duration) in elapsed_duration.iter().enumerate() {
        let no_of_operations = no_of_items + no_of_items * (index + 1);
        println!("TIME TAKEN {:?}", duration.as_micros());
        let throughput = no_of_operations as f64 / duration.as_micros() as f64;
        println!("THROUGHPUT {}", throughput);
        throughput_values.push((100.0 / get_per_puts.clone()[index] as f64, throughput));
        println!("THE % put is {}", 100.0 / get_per_puts.clone()[index] as f64);
    }

    generate_throughput_graph(throughput_values, "Single lock", "single_lock.png");

    // Striped lock server
    let mut elapsed_duration = vec![];
    for get_per_put in get_per_puts.clone() {
        elapsed_duration.push(evaluate_hashtable(striped_lock_server, no_of_threads, input.clone(), get_per_put));
    }
    
    let mut throughput_values = vec![];
    for (index, duration) in elapsed_duration.iter().enumerate() {
        let no_of_operations = no_of_items + no_of_items * (index + 1);
        println!("TIME TAKEN {:?}", duration.as_micros());
        let throughput = no_of_operations as f64 / duration.as_micros() as f64;
        println!("THROUGHPUT {}", throughput);
        // Append through put values
        throughput_values.push((100.0 / get_per_puts.clone()[index] as f64, throughput));
        println!("THE % put is {}", 100.0 / get_per_puts.clone()[index] as f64);
    }
    generate_throughput_graph(throughput_values, "Striped lock", "striped_lock.png");
}

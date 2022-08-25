use std::io::prelude::*;
use std::io;
use std::net::TcpStream;
use std::io::BufReader;

fn main() {
    
    let server: String = std::env::args().nth(1).expect("No server given");

    // Create a infinite loop
    loop {

        let mut command = String::new();

        // Get the input - for command
        let mut operation = String::new();
        io::stdin().read_line(&mut operation).expect("Failed to readline");
        

        let mut key_str = String::new();
        io::stdin().read_line(&mut key_str).expect("Failed to readline");
        if operation.trim().eq("PUT"){
            // get the value to be inserted
            let mut value_str = String::new();
            io::stdin().read_line(&mut value_str).expect("Failed to readline");
            command = format!("{} {} {}\n",operation.trim(), key_str.trim(), value_str.trim());
        }
        else if operation.trim().eq("GET") {
            command = format!("{} {}\n",operation.trim(), key_str.trim());
        }
        else{ 
            // Print wrong command and start from first;
            println!("Enter correct operation - Either PUT or GET");
            continue;
        }
        let mut stream = TcpStream::connect(server.clone()).unwrap();
        // Send the commands
        stream.write(command.as_bytes()).unwrap();

        // Parse and display the output.
        let mut reader = BufReader::new(&stream);
        let mut error_code = String::new();
        reader.read_line(&mut error_code).unwrap();
        // Check error code
        if error_code.trim().parse::<i32>().unwrap() == 0 {
            println!("Suceeded");
            let mut value = String::new();
            reader.read_line(&mut value).unwrap();
            if operation.trim().eq("GET") {
                println!("The value for {} is {}", key_str.trim(), value);
            }
            else if operation.trim().eq("PUT") {
                println!("{}", value.trim());
            }
        }
        else {
            println!("FAILED");
            let mut error_value = String::new();
            reader.read_line(&mut error_value).unwrap();
            println!("{}", error_value);
        }
    }
}

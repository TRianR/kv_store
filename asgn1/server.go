package main

import (
"fmt";
"net";
"os";
)

func main() {
var (
host = "127.0.0.1";
port = "5000";
remote = host + ":" + port;
data = make([]byte, 1024);
)
fmt.Println("Starting server... ");

lis, error := net.Listen("tcp", remote);
defer lis.Close();
if error != nil {
fmt.Printf("Error creating listener: %s\n", error );
os.Exit(1);
}
/*
var rollnumber = map[string]string{
    "Nilesh":  "100050040",
    "Pulkit": "100050043",
    "Ankush": "100050042",
}*/
for {
var response string;
con, error := lis.Accept();
if error != nil { fmt.Printf("Error: Accepting data: %s\n", error); os.Exit(2); }
fmt.Printf("New Connection received from: %s \n", con.RemoteAddr());
n, error := con.Read(data);
switch error {
case nil:
//fmt.Println(string(data[0:n])); // Debug
response = response + string(data[0:n]);
default:
fmt.Printf("Error: Reading data : %s \n", error);
}

fmt.Println("Data recieved: " + response);
con.Close();
}
}

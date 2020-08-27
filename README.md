A Golang 1.12.4 application that contains the code for Kafka consumer for creating a Kafka Consumer that can connect to the source of truth (in this project it is Postgres). This project runs as a continuous worker which subscribes to a topic in kafka and reads from the last unread offset and inserts the parsed data into postgres table.

This application uses govendor to manage the vendoring of dependencies. When pushing any code to this repository please add the dependency used in the vendor/vendor.json. 
Please make sure you remove any unused dependencies from code as well as vendor/vendor.json because any unused dependency will throw errors.


Steps to setup and use the application locally

    1. Install go
    2. Update your GOPATH to use the project directory
    3. Install govendor.  ‘$go get -u github.com/kardianos/govendor’
   
        3.1 Getting started with govendor
            $govendor init
            Inspect the changes with $git diff (or similar).
            Commit the changes with $git add -A vendor; $git commit -am "Setup Vendor"

        3.2 Adding dependencies
            $govendor fetch <package>
            Inspect the changes with $git diff (or similar).
            Commit the changes with $git add -A vendor; $git commit -am "Add dependency <package>"

    4. Update config/config.txt
    5. $go run main.go to test

List of dependencies currently being used
    
    1. github.com/namsral/flag : parse files and environment variables
    2. github.com/segmentio/kafka-go : library for kafka 
    3. github.com/lib/pq : go postgres driver for database
    4. github.com/lib/pq/oid : connection to the postgres database







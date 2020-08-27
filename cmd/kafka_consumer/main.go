package main

import (
	"reflect"
	"context"
    "strconv"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"encoding/json"
	"github.com/namsral/flag"
    "github.com/segmentio/kafka-go"
	"database/sql"
	_ "github.com/lib/pq"
    "log"
    "bufio"
    "regexp"
)

type KafkaConsumer struct {
        KafkaConsumerTime int64 `json:"timestamp"`
        UserId int64 `json:"user_id"`
        UserType string `json:"user_type"`
        KafkaConsumerDetails map[string]interface{} `json:"-"`
        UserName string `json:"user_name"`
}

var config map[string]string
var conf_file string

type _KafkaConsumer KafkaConsumer

var (
	kafkaBrokerUrl     string
	kafkaVerbose       bool
	kafkaTopic         string
	kafkaConsumerGroup string
	kafkaClientId      string
)

func main() {

  //fetch values from environment variables if it is not present then fetch it from config/config.txt file
            conf_file = "config/config.txt"
            config = ReadConfig(conf_file)
            host := config["host"]
            kafka_broker_url := config["kafka_broker_url"]
            user     := config["user"]
            password := config["password"]
            dbname   := config["dbname"]
            kafka_topic := config["kafka_topic"]
            kafka_consumer_group := config["kafka_consumer_group"]
            kafka_client_id := config["kafka_consumer_group"]
            port, err_int     := strconv.Atoi(config["port"])
            if err_int !=nil{
                fmt.Println(err_int)
            }

            if os.Getenv("kafka_broker_url") != ""{
                kafka_broker_url = os.Getenv("kafka_broker_url")
            }
            if os.Getenv("host") != ""{
                host = os.Getenv("host")
            }
            if os.Getenv("user") != ""{
                user = os.Getenv("user")
            }
            if os.Getenv("password") != ""{
                password = os.Getenv("password")
            }
            if os.Getenv("dbname") != ""{
                dbname = os.Getenv("dbname")
            }
            if os.Getenv("kafka_topic") != ""{
                kafka_topic = os.Getenv("kafka_topic")
            }
            if os.Getenv("kafka_consumer_group") != ""{
                kafka_consumer_group = os.Getenv("kafka_consumer_group")
            }
            if os.Getenv("kafka_client_id") != ""{
                kafka_client_id = os.Getenv("kafka_client_id")
            }

            if os.Getenv("db_port") != ""{
                port, err_int = strconv.Atoi(os.Getenv("db_port"))
                if err_int !=nil{
                    fmt.Println(err_int)
                }
            }

            fmt.Println(kafka_topic);

  //Kafka consumer group config with broker url, verbose, kafka-topic, consumer group, client-id
            flag.StringVar(&kafkaBrokerUrl, "kafka-brokers", kafka_broker_url, "Kafka brokers in comma separated value")
            flag.BoolVar(&kafkaVerbose, "kafka-verbose", true, "Kafka verbose logging")
            flag.StringVar(&kafkaTopic, "kafka-topic", kafka_topic, "Kafka topic. Only one topic per worker.")
            flag.StringVar(&kafkaConsumerGroup, "kafka-consumer-group", kafka_consumer_group, "Kafka consumer group")
            flag.StringVar(&kafkaClientId, "kafka-client-id", kafka_client_id, "Kafka client id")

            flag.Parse()

            sigchan := make(chan os.Signal, 1)
            signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

            brokers := strings.Split(kafkaBrokerUrl, ",")

            // make a new reader that consumes from kafka-topic
            config := kafka.ReaderConfig{
                Brokers:         brokers,
                GroupID:         kafkaClientId,
                Topic:           kafkaTopic,
                MinBytes:        10e3,            // 10KB
                MaxBytes:        10e6,            // 10MB
                MaxWait:         1 * time.Second, // Maximum amount of time to wait for new data to come when fetching batches of messages from kafka.
                ReadLagInterval: -1,
            }

            reader:= kafka.NewReader(config)
            fmt.Println(reader)
            defer reader.Close()
            fmt.Println("reader closed")

            psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
               "password=%s dbname=%s sslmode=disable",
                host, port, user, password, dbname)
            db, err_db := sql.Open("postgres", psqlInfo)
            if err_db != nil {
                            fmt.Println("error encountered while estasblishing connection to the database")
                            fmt.Println(err_db)
            }
            db.SetMaxIdleConns(10)
            db.SetMaxOpenConns(10)

            defer db.Close()
            for{
                      m, err := reader.ReadMessage(context.Background())
                      if err != nil {
                      //			log.Error().Msgf("error while receiving message: %s", err.Error())
                                fmt.Println(err)
                                continue
                      }

                      value := m.Value

                      var aud KafkaConsumer

                      error_r := json.Unmarshal(value, &aud)
                      if error_r != nil {
                            fmt.Println("here")
                            fmt.Println(error_r)
                      }

                      err_db = db.Ping()
                      if err_db != nil {
                            fmt.Println("error connecting to the database")
                            fmt.Println(err_db)
                      }

                      fmt.Println("Successfully connected!")

                 //write the kafka message queue to postgres
                    jsonString, error_details := json.Marshal(aud.KafkaConsumerDetails)
                    s := string(jsonString)
                    s = s[11 : len(s)-1]
                    fmt.Println(error_details)

                    sqlStatement := `INSERT INTO user (created_at, user_id, user_type, details, user_name)
                                    VALUES ($1, $2, $3, $4, $5)
                                    RETURNING id`
                    id := 0

                    err_db = db.QueryRow(sqlStatement, aud.KafkaConsumerTime, aud.UserId, aud.UserType, s, aud.UserName).Scan(&id)
                    fmt.Println("New record ID is:", id)
                               
                    fmt.Printf("message at topic/partition/offset %v/%v/%v: \n", m.Topic, m.Partition, m.Offset)
                    fmt.Println(aud)
	        }
}

// function to process dynamic json like detail
func (t *KafkaConsumer) UnmarshalJSON(b []byte) error {
        t2 := _KafkaConsumer{}
        err := json.Unmarshal(b, &t2)
        if err != nil {
            return err
        }

        err = json.Unmarshal(b, &(t2.KafkaConsumer))
        if err != nil {
            return err
        }

        typ := reflect.TypeOf(t2)
        for i := 0; i < typ.NumField(); i++ {
            field := typ.Field(i)
            jsonTag := strings.Split(field.Tag.Get("json"), ",")[0]
            if jsonTag != "" && jsonTag != "-" {
                delete(t2.KafkaCosumerDetails, jsonTag)
            }
        }

        *t = KafkaConsumer(t2)

        return nil
}

func ReadConfig(filename_fullpath string) map[string]string {
        prg := "ReadConfig()"

        var options map[string]string
        options = make(map[string]string)

        file, err := os.Open(filename_fullpath)
        if err != nil {
              log.Printf("%s: os.Open(): %s\n", prg, err)
              return options
        }
        defer file.Close()

        scanner := bufio.NewScanner(file)
        for scanner.Scan() {
            line := scanner.Text()
            if strings.Contains(line, "=") == true {
                  re, err := regexp.Compile(`([^=]+)=(.*)`)
                  if err != nil {
                        log.Printf("%s: regexp.Compile(): error=%s", prg, err)
                        return options
                  } else {
                        config_option := re.FindStringSubmatch(line)[1]
                        config_value := re.FindStringSubmatch(line)[2]
                        options[config_option] = config_value
                        //log.Printf("%s: out[]: %s ... config_option=%s, config_value=%s\n", prg, line, config_option, config_value)
                  }
            }
        }
        //log.Printf("%s: options[]: %+v\n", prg, options)

        if err := scanner.Err(); err != nil {
            log.Printf("%s: scanner.Err(): %s\n", prg, err)
            return options
        }
  return options
}


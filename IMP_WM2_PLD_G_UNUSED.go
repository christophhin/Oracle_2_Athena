package main

import (
  "fmt"
  "os"
  "path/filepath"
  "gopkg.in/ini.v1"
  "database/sql"
  "encoding/csv"
  "net/http"
  "io/ioutil"
  "bytes"
  "os/exec"
  "strconv"
  "encoding/json"
  _ "github.com/godror/godror"
)

type INI struct {
  host         string
  port         int
  user         string
  pswd         string
  dbase        string
  table        string
  url          string
  username     string
  accountId    string
  roleName     string
  password     string
  region       string
  s3Prefix     string
  logOutPrefix string
}

type Payload struct {
  Username  string `json:"username"`
  AccountID string `json:"accountId"`
  RoleName  string `json:"roleName"`
  Password  string `json:"password"`
}

type Response struct {
  AwsAccessKey    string `json:"awsAccessKey"`
  AwsSecretKey    string `json:"awsSecretKey"`
  AwsSessionToken string `jsin:"awsSessionToken"`
}

type IMP_WM2_PLD_G_UNUSED struct {
  WKN              string
  RECORD_NUMBER    int64
  FIELD_ID         string
  FIELD_VALUE      string
  DATE_ENTRY       string
  DATE_LAST_CHANGE string
  IS_EFFECTIVE     int64
}

func readINI() INI {
  // --- find ini file ---
  file, _ := os.Readlink("/proc/self/exe")

  // --- read ini file ---
  cfg, err := ini.Load(filepath.Join(filepath.Dir(file), "/IMP_WM2_PLD_G_UNUSED.ini"))
  if err != nil {
    panic(err.Error())
  }

  port, err := cfg.Section("oracle").Key("port").Int()
  ini := INI {
                cfg.Section("oracle").Key("host").String(),
                port,
                cfg.Section("oracle").Key("user").String(),
                cfg.Section("oracle").Key("pswd").String(),
                cfg.Section("oracle").Key("dbase").String(),
                cfg.Section("oracle").Key("table").String(),
                cfg.Section("main").Key("url").String(),
                cfg.Section("main").Key("username").String(),
                cfg.Section("main").Key("accountId").String(),
                cfg.Section("main").Key("roleName").String(),
                cfg.Section("main").Key("password").String(),
                cfg.Section("main").Key("region").String(),
                cfg.Section("main").Key("s3Prefix").String(),
                cfg.Section("main").Key("logOutPrefix").String(),
             }
  return ini
}

func getS3creds(ini INI) {
  var response Response

  data := Payload{
    ini.username,
    ini.accountId,
    ini.roleName,
    ini.password,
  }
  
  payloadBytes, err := json.Marshal(data)
  if err != nil {
    panic(err.Error())
  }
  body := bytes.NewReader(payloadBytes)
  req, err := http.NewRequest("POST", ini.url, body)
  if err != nil {
    panic(err.Error())
  }
  req.Header.Set("Content-Type", "application/json")
  resp, err := http.DefaultClient.Do(req)
  if err != nil {
    panic(err.Error())
  }
  defer resp.Body.Close()    

  bodyBytes, err := ioutil.ReadAll(resp.Body)
  
  err = json.Unmarshal(bodyBytes, &response)
  if err != nil {
    panic(err.Error())
  }

  // --- create aws cli config file ---
  config := "/root/.aws/credentials"
  f, err := os.Create(config)
  if err != nil {
    panic(err.Error())
  }

  fmt.Fprintln(f, "[default]")
  fmt.Fprintln(f, fmt.Sprintf("aws_access_key_id = %s", response.AwsAccessKey))
  fmt.Fprintln(f, fmt.Sprintf("aws_secret_access_key = %s", response.AwsSecretKey))
  fmt.Fprintln(f, fmt.Sprintf("aws_session_token = %s", response.AwsSessionToken))
  f.Close()
  err = os.Chmod(config, 0600)
  if err != nil {
    panic(err.Error())
  }
}

func main() {
  ini := readINI()
  getS3creds(ini)

  // --- check command line arguments ---
  if len(os.Args) != 3 {
    fmt.Println("Usage:")
    fmt.Printf("   %s year month\n", os.Args[0])
    os.Exit(-1)
  }

  // --- open db connection ---
  dsn := fmt.Sprintf("%s/%s@%s:%d/%s",
                        ini.user,
                        ini.pswd,
                        ini.host,
                        ini.port,
                        ini.dbase)
  db, err := sql.Open("godror", dsn)
  if err != nil {
    panic(err.Error())
  }
  defer db.Close()

  // --- execute query ---
  y, _ := strconv.Atoi(os.Args[1])
  m, _ := strconv.Atoi(os.Args[2])
  
  sql := fmt.Sprintf(`SELECT
                        WKN,
                        RECORD_NUMBER,
                        FIELD_ID,
                        FIELD_VALUE,
                        TO_CHAR(DATE_ENTRY, 'YYYY-MM-DD HH24:MI:SS') AS DATE_ENTRY,
                        TO_CHAR(DATE_LAST_CHANGE, 'YYYY-MM-DD HH24:MI:SS') AS DATE_LAST_CHANGE,
                        IS_EFFECTIVE
                      FROM
                        %s
                      WHERE
                        EXTRACT(year FROM DATE_ENTRY) = %d AND
                        EXTRACT(month FROM DATE_ENTRY) = %d`,
                     ini.table, y, m)

  results, err := db.Query(sql)
  if err != nil {
    panic(err.Error())
  }

  // --- create csv output file and start writer ---
  outFileName := "/tmp/IMP_WM2_PLD_V_UNUSED.csv"
  outFile, err := os.Create(outFileName)
  if err != nil {
    panic(err.Error())
  }
  parserOut := csv.NewWriter(outFile)

  i := 0;
  for results.Next() {
    var record IMP_WM2_PLD_G_UNUSED

    err = results.Scan( &record.WKN,
                        &record.RECORD_NUMBER,
                        &record.FIELD_ID,
                        &record.FIELD_VALUE,
                        &record.DATE_ENTRY,
                        &record.DATE_LAST_CHANGE,
                        &record.IS_EFFECTIVE)
     if err != nil {
      panic(err.Error())
    }

    i++
    fmt.Printf("%d\n", i)

    rec := []string {
      fmt.Sprintf("%s", record.WKN),
      fmt.Sprintf("%d", record.RECORD_NUMBER),
      fmt.Sprintf("%s", record.FIELD_ID),
      fmt.Sprintf("%s", record.FIELD_VALUE),
      fmt.Sprintf("%s", record.DATE_ENTRY),
      fmt.Sprintf("%s", record.DATE_LAST_CHANGE),
      fmt.Sprintf("%s", record.IS_EFFECTIVE),
    }
    
		if err = parserOut.Write(rec); err != nil {
			fmt.Println("Write error", err)
		  panic(err.Error())
		}
  }
  parserOut.Flush()
  outFile.Close()

  s3Target := fmt.Sprintf("%s/year=%s/month=%s/IMP_WM2_PLD_G_UNUSED.csv", ini.s3Prefix, os.Args[1], os.Args[2])

  // --- execute copy to s3 storage ---
  args := []string{"s3", "cp", outFileName, s3Target}
  out, err := exec.Command("/usr/local/bin/aws", args...).Output()
  if err != nil {
    panic(err.Error())
    os.Exit(1)
  }
  
  fmt.Printf("Copy executed: %s\n", string(out))

  // --- delete local client source file ---
  err = os.Remove(outFileName)
  if err != nil {
    panic(err.Error())
    os.Exit(1)
  }
}

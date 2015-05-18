package main

import (
    "fmt"
    "log"
    "strings"
    "os"
    "net/http"
    "archive/zip"
    "io"
    "io/ioutil"
    "bufio"
    "errors"
    "database/sql"
    _ "github.com/lib/pq"
)

// USAGE: go run form5500-data-sets-import.go form5500_data_sets 2013 http://askebsa.dol.gov/FOIA%20Files/ ./
// USAGE: go run form5500-data-sets-import.go fbi_development 2013 http://askebsa.dol.gov/FOIA%20Files/ ./
func main() {
    dbName := os.Args[1]
    year := os.Args[2]
    baseUrl := os.Args[3]

    connection := fmt.Sprintf("host=localhost port=5432 dbname=%s sslmode=disable", dbName)

    db, err := sql.Open("postgres", connection)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    name := "f_5500_%s_latest"

    tableName, err := createTable(db, baseUrl, name, year)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Created table: " + tableName)

    csvFilename, err := downloadCSV(db, baseUrl, name, year)
    if err != nil {
        log.Fatal(err)
    }
    defer os.Remove(csvFilename)
    fmt.Println("Created CSV file: " + csvFilename)
}

func downloadCSV(db *sql.DB, baseUrl, name string, year string) (string, error) {
    name = fmt.Sprintf(name, year)
    url := baseUrl + fmt.Sprintf("%s/Latest/%s.zip", year, name)

    zipFilename, err := downloadFile(name, url)
    if err != nil {
        log.Fatal(err)
    }
    defer os.Remove(zipFilename)

    r, err := zip.OpenReader(zipFilename)
    if err != nil {
        log.Fatal(err)
    }
    defer r.Close()

    csvFilename := strings.ToLower(name) + ".csv"

    for _, f := range r.File {
        if f.Name == csvFilename {
            csvFile, err := f.Open()
            if err != nil {
                log.Fatal(err)
            }
            defer csvFile.Close()

            tempFile, tempFilename, err := createTempFile(csvFilename)
            if err != nil {
                return "", err
            }
            defer tempFile.Close()

            _, err = io.Copy(tempFile, csvFile)
            if err != nil {
                log.Fatal(err)
            }

            return tempFilename, nil
        }
    }

    return "", errors.New("CSV not found in ZIP file at " + url)
}

func createTable(db *sql.DB, baseUrl, name string, year string) (string, error) {
    tableName := fmt.Sprintf(name, year)
    url := baseUrl + fmt.Sprintf("%s/Latest/%s_layout.txt", year, tableName)

    resp, err := http.Get(url)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    scanner := bufio.NewScanner(resp.Body)

    // eat first two header lines
    scanner.Scan()
    scanner.Scan()

    sqlLines := make([]string, 0)
    sqlLines = append(sqlLines, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", tableName))
    sqlLines = append(sqlLines, fmt.Sprintf("CREATE TABLE %s (", tableName))

    for scanner.Scan() {
        line := scanner.Text()
        parts := strings.Split(line, ",")

        switch {
            case parts[1] == "ACK_ID":
                sqlLines = append(sqlLines, `    "ACK_ID" varchar(30) PRIMARY KEY`)
    
            case len(parts) == 4:
                sqlLines[len(sqlLines)-1] = sqlLines[len(sqlLines)-1] + ","
                if parts[3] == "1" && strings.HasSuffix(parts[1], "_IND") {
                    sqlLines = append(sqlLines, fmt.Sprintf(`    "%s" bool`, parts[1]))
                } else {
                    sqlLines = append(sqlLines, fmt.Sprintf(`    "%s" varchar(%s)`, parts[1], parts[3]))
                }

            case parts[2] == "NUMERIC":
                sqlLines[len(sqlLines)-1] = sqlLines[len(sqlLines)-1] + ","
                if parts[1] == "ROW_ORDER" || strings.HasSuffix(parts[1], "_CNT") {
                    sqlLines = append(sqlLines, fmt.Sprintf(`    "%s" int`, parts[1]))
                } else {
                    sqlLines = append(sqlLines, fmt.Sprintf(`    "%s" numeric(19,6)`, parts[1]))
                }
        }
    }
    sqlLines = append(sqlLines, ");")

    if err := scanner.Err(); err != nil {
        fmt.Fprintln(os.Stderr, "reading standard input:", err)
    }

    sql := ""
    for _, line := range sqlLines {
        sql = sql + line + "\n"
    }

    _, err = db.Exec(sql)
    if err != nil {
        return "", err
    }

    return tableName, nil
}

func downloadFile(prefix string, url string) (string, error) {
    tempFile, tempFilename, err := createTempFile(prefix)
    if err != nil {
        return "", err
    }
    defer tempFile.Close()

    resp, err := http.Get(url)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    _, err = io.Copy(tempFile, resp.Body)
    if err != nil {
        return "", err
    }

    return tempFilename, nil
}

func createTempFile(prefix string) (*os.File, string, error) {
    tempFile, err := ioutil.TempFile("", prefix)
    if err != nil {
        return nil, "", err
    }
    return tempFile, tempFile.Name(), nil
}

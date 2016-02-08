package main

import (
    "fmt"
    "log"
    "strings"
    "os"
    "os/exec"
    "net/http"
    "archive/zip"
    "io"
    "io/ioutil"
    "bufio"
    "errors"
    "database/sql"
    _ "github.com/lib/pq"
)

// USAGE: go run form5500-data-sets-import.go "host=localhost port=5432 dbname=form5500_data_sets sslmode=disable" 2013 http://askebsa.dol.gov/FOIA%20Files/
// USAGE: go run form5500-data-sets-import.go "host=localhost port=5432 dbname=fbi_development sslmode=disable" 2013 http://askebsa.dol.gov/FOIA%20Files/

func main() {
    connection := os.Args[1]
    year := os.Args[2]
    baseUrl := os.Args[3]

    db, err := sql.Open("postgres", connection)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    names := []string{
        "f_5500_%s_latest", 
        "f_5500_sf_%s_latest",
        "f_sch_a_%s_latest",
        "f_sch_a_part1_%s_latest",
        "f_sch_c_%s_latest",
        "f_sch_c_part1_item1_%s_latest",
        "f_sch_c_part1_item2_%s_latest",
        "f_sch_c_part1_item2_codes_%s_latest",
        "f_sch_c_part1_item3_%s_latest",
        "f_sch_c_part1_item3_codes_%s_latest",
        "f_sch_c_part2_%s_latest",
        "f_sch_c_part2_codes_%s_latest",
        "f_sch_c_part3_%s_latest",
        "f_sch_d_%s_latest",
        "f_sch_d_part1_%s_latest",
        "f_sch_d_part2_%s_latest",
        "f_sch_g_%s_latest",
        "f_sch_g_part1_%s_latest",
        "f_sch_g_part2_%s_latest",
        "f_sch_g_part3_%s_latest",
        "f_sch_g_%s_latest",
        "f_sch_g_%s_latest",
        "f_sch_g_%s_latest",
        "f_sch_h_%s_latest",
        "f_sch_h_part1_%s_latest",
        "f_sch_i_%s_latest",
        "f_sch_r_%s_latest",
        "f_sch_r_part1_%s_latest",
        "f_sch_mb_%s_latest",
        "f_sch_mb_part1_%s_latest",
        "f_sch_mb_part2_%s_latest",
        "f_sch_sb_%s_latest",
        "f_sch_sb_part1_%s_latest",
    }

    for _, name := range names {
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

        err = importCSV(connection, tableName, csvFilename)
    }
}

func importCSV(connection string, tableName string, csvFilename string) (error) {
    s := fmt.Sprintf(`TRUNCATE %s`, tableName)
    fmt.Println("psql \"" + connection + "\" -c \"" + s + "\"")
    cmd := exec.Command("psql", connection, "-c", s)
    output, err := cmd.Output()
    if err != nil {
        return err
    }
    fmt.Println(string(output))

    s = fmt.Sprintf(`\copy %s FROM '%s' DELIMITER ',' CSV HEADER`, tableName, csvFilename)
    fmt.Println("psql \"" + connection + "\" -c \"" + s + "\"")
    cmd = exec.Command("psql", connection, "-c", s)
    output, err = cmd.Output()
    if err != nil {
        return err
    }
    fmt.Println(string(output))
    return nil
}

func downloadCSV(db *sql.DB, baseUrl, name string, year string) (string, error) {
    name = fmt.Sprintf(name, year)
    url := baseUrl + fmt.Sprintf("%s/Latest/%s.zip", year, name)

    zipFilename, err := downloadFile(name, url)
    if err != nil {
        log.Fatal(err)
    }
    defer os.Remove(zipFilename)

    reader, err := zip.OpenReader(zipFilename)
    if err != nil {
        log.Fatal(err)
    }
    defer reader.Close()

    csvFilename := strings.ToLower(name) + ".csv"

    for _, f := range reader.File {
        if strings.ToLower(f.Name) == csvFilename {
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

    // first column is always ACK_ID
    scanner.Scan()
    sqlLines = append(sqlLines, `    "ACK_ID" varchar(30)`)

    for scanner.Scan() {
        line := scanner.Text()
        parts := strings.Split(line, ",")

        sqlLines[len(sqlLines)-1] = sqlLines[len(sqlLines)-1] + ","

        switch {
            case strings.HasSuffix(parts[1], "_DATE") || parts[1] == "FORM_TAX_PRD":
                sqlLines = append(sqlLines, fmt.Sprintf(`    "%s" timestamp`, parts[1]))

            case len(parts) == 4:
                if parts[3] == "1" && strings.HasSuffix(parts[1], "_IND") {
                    sqlLines = append(sqlLines, fmt.Sprintf(`    "%s" int`, parts[1]))
                } else {
                    if parts[3] == "0" {
                        sqlLines = append(sqlLines, fmt.Sprintf(`    "%s" char`, parts[1]))
                    } else {
                        sqlLines = append(sqlLines, fmt.Sprintf(`    "%s" varchar(%s)`, parts[1], parts[3]))
                    }
                }

            case parts[2] == "NUMERIC":
                if parts[1] == "ROW_ORDER" {
                    sqlLines = append(sqlLines, fmt.Sprintf(`    "%s" int`, parts[1]))
                } else if strings.HasSuffix(parts[1], "_CNT") {
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

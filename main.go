package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jamf/go-mysqldump"
	"github.com/joho/godotenv"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/drive/v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	dumpDir := "dumps"
	dumpName := fmt.Sprintf("%s-20060102", "dms")
	date := time.Now().Format("20060102")
	formatName := fmt.Sprintf("%s-%s.sql", dumpDir, date)

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN: os.Getenv("DATABASE_CONNECTION_STRING"),
	}), &gorm.Config{
		SkipDefaultTransaction: true,
	})
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}

	sqlDB, err := db.DB()
	if err != nil {
		fmt.Println("Error getting underlying *sql.DB:", err)
		return
	}

	if _, err := os.Stat(dumpDir); os.IsNotExist(err) {
		err := os.Mkdir(dumpDir, os.ModePerm)
		if err != nil {
			fmt.Println("Error creating dump directory:", err)
			return
		}
	}

	dumper, err := mysqldump.Register(sqlDB, dumpDir, dumpName)
	if err != nil {
		fmt.Println("Error registering database:", err)
		return
	}

	err = dumper.Dump()
	if err != nil {
		fmt.Println("Error dumping:", err)
		return
	}

	fmt.Printf("File is saved to %s \n", formatName)
	dumper.Close()
	cmd := exec.Command("perl", "-pi", "-e", "s/ %!s\\(bool=true\\)\\}//g; s/&\\{//g; s/ %!s\\(bool=false\\)\\}//g;", fmt.Sprintf("%s/dms-%s.sql", dumpDir, date))

	err = cmd.Run()
	if err != nil {
		fmt.Println("Error running Perl command:", err)
		return
	}
	uploadeFile()
}

func ServiceAccount(secretFile string) *http.Client {
	b, err := ioutil.ReadFile(secretFile)
	if err != nil {
		log.Fatal("error while reading the credential file", err)
	}
	var s = struct {
		Email      string `json:"client_email"`
		PrivateKey string `json:"private_key"`
	}{}
	json.Unmarshal(b, &s)
	config := &jwt.Config{
		Email:      s.Email,
		PrivateKey: []byte(s.PrivateKey),
		Scopes: []string{
			drive.DriveScope,
		},
		TokenURL: google.JWTTokenURL,
	}
	client := config.Client(context.Background())
	return client
}

func createFile(service *drive.Service, filePath string, mimeType string, content io.Reader, parentId string) (*drive.File, error) {
	fileName := filepath.Base(filePath) 

	f := &drive.File{
		MimeType: mimeType,
		Name:     fileName, 
		Parents:  []string{parentId},
	}
	file, err := service.Files.Create(f).Media(content).Do()

	if err != nil {
		log.Println("Could not create file: " + err.Error())
		return nil, err
	}

	return file, nil
}

func uploadeFile() {
	formatName := fmt.Sprintf("dms-%s.sql", time.Now().Format("20060102"))
	filePath := fmt.Sprintf("dumps/%s", formatName)

	f, err := os.Open(filePath)
	if err != nil {
		panic(fmt.Sprintf("cannot open file: %v", err))
	}
	defer f.Close()

	client := ServiceAccount("credentials.json")
	srv, err := drive.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve drive Client %v", err)
	}
	folderId := os.Getenv("FOLDER_ID")

	file, err := createFile(srv, f.Name(), "application/octet-stream", f, folderId)

	if err != nil {
		panic(fmt.Sprintf("Could not create file: %v\n", err))
	}
	fmt.Printf("File '%s' successfully uploaded", file.Name)
	fmt.Printf("\nFile Id: '%s' \n", file.Id)
}

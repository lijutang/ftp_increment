package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jlaffaye/ftp"
)

const (
	ftpServer         = "192.168.31.243"
	ftpUser           = "ftpuser"
	ftpPass           = "ftpuser"
	localFilePath     = "E:/workspace/go_dev/test/ftp_increment/uploaded/file1.txt"
	remoteFilePath    = "/home/ftpuser/shared/file1.txt"
	offsetFilePath    = "E:/workspace/go_dev/test/ftp_increment/uploaded/last_uploaded_offset.txt" // 假设这是存储最后上传偏移量的文件
	tempIncrementFile = "E:/workspace/go_dev/test/ftp_increment/uploaded/increment.tmp"            // 增量数据的临时文件名
)

func main() {

	// 连接到FTP服务器
	c, err := ftp.Dial(fmt.Sprintf("%s:%d", ftpServer, 21))
	if err != nil {
		log.Fatalf("Failed to connect to FTP server: %v", err)
	}
	defer c.Quit()

	// 登录到FTP服务器
	if err := c.Login(ftpUser, ftpPass); err != nil {
		log.Fatalf("Failed to login to FTP server: %v", err)
	}

	offset, err := readLastUploadedOffset(offsetFilePath)
	if err != nil {
		log.Fatalf("Failed to read last uploaded offset: %v", err)
	}

	localFile, err := os.Open(localFilePath)
	if err != nil {
		log.Fatalf("Failed to open local file: %v", err)
	}
	defer localFile.Close()

	tmpFile, err := os.Create(tempIncrementFile)
	if err != nil {
		log.Fatalf("Failed to create temporary file for increments: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // 清理临时文件
	defer tmpFile.Close()

	for {
		// 模拟新增日志
		appendLog(localFilePath)

		// 将文件指针移动到上次上传的偏移量位置
		_, err = localFile.Seek(offset, 0)
		if err != nil {
			log.Fatalf("Failed to seek in local file: %v", err)
		}

		// 清空临时文件内容
		tmpFile.Seek(0, 0)
		tmpFile.Truncate(0)

		// 复制增量数据到临时文件
		_, err = io.Copy(tmpFile, localFile)
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to copy increment data to temporary file: %v", err)
		}

		// 重置临时文件和本地文件指针到开头，为下次读取做准备
		_, err = tmpFile.Seek(0, 0)
		if err != nil {
			log.Fatalf("Failed to seek temporary file: %v", err)
		}
		_, err = localFile.Seek(0, 2) // 移动到文件末尾以准备下一次追加
		if err != nil {
			log.Fatalf("Failed to seek to the end of local file: %v", err)
		}

		// 获取新的偏移量
		newOffset, err := localFile.Seek(0, 1)
		if err != nil {
			log.Fatalf("Failed to get new offset: %v", err)
		}
		// 连接到FTP服务器
		c, err := ftp.Dial(fmt.Sprintf("%s:%d", ftpServer, 21))
		if err != nil {
			log.Fatalf("Failed to connect to FTP server: %v", err)
		}
		defer c.Quit()

		// 登录到FTP服务器
		if err := c.Login(ftpUser, ftpPass); err != nil {
			log.Fatalf("Failed to login to FTP server: %v", err)
		}

		// 增量上传
		if err := uploadIncrement(c, tmpFile, remoteFilePath); err != nil {
			log.Fatalf("Failed to upload increment data to FTP server: %v", err)
		}

		// 更新偏移量文件
		if err := writeLastUploadedOffset(offsetFilePath, newOffset); err != nil {
			log.Fatalf("Failed to update last uploaded offset: %v", err)
		}

		offset = newOffset // 更新偏移量
		fmt.Printf("Uploaded increment data (new offset: %d bytes)\n", offset)

		// 暂停以模拟日志生成
		time.Sleep(4 * time.Second)

	}
}

// uploadIncrement 上传临时文件中的增量数据到FTP服务器
func uploadIncrement(c *ftp.ServerConn, tmpFile *os.File, remotePath string) error {
	// 使用APPE命令追加数据到远程文件
	err := c.Append(remotePath, tmpFile)
	if err != nil {
		return err
	}
	return nil
}

// 读取上次上传的偏移量
func readLastUploadedOffset(filePath string) (int64, error) {
	data, err := os.ReadFile(filePath)
	if os.IsNotExist(err) {
		// 如果文件不存在，返回0偏移量，而不是错误
		return 0, nil
	}
	if err != nil {
		// 对于其他错误，仍然返回错误
		return 0, err
	}
	offset, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0, err
	}
	return offset, nil
}

// 写入新的上传偏移量
func writeLastUploadedOffset(filePath string, offset int64) error {
	return os.WriteFile(filePath, []byte(fmt.Sprintf("%d", offset)), 0644)
}

// appendLog 模拟增加日志内容
func appendLog(filePath string) {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file for appending: %v", err)
	}
	defer f.Close()

	logEntry := fmt.Sprintf("Log entry at %s\n", time.Now().Format(time.RFC3339))
	if _, err := f.WriteString(logEntry); err != nil {
		log.Fatalf("Failed to write log entry: %v", err)
	}
	time.Sleep(2 * time.Second)
}

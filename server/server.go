
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
	ftpServer      = "192.168.31.243"
	ftpUser        = "ftpuser"
	ftpPass        = "ftpuser"
	localFilePath  = "E:/workspace/go_dev/test/ftp_increment/download/file1.txt"
	remoteFilePath = "/home/ftpuser/shared/file1.txt"
	offsetFilePath = "E:/workspace/go_dev/test/ftp_increment/download/last_downloaded_offset.txt"
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

	offset, err := readLastDownloadedOffset(offsetFilePath)
	if err != nil {
		log.Fatalf("Failed to read last downloaded offset: %v", err)
	}

	for {
		// 检查远程文件的大小
		remoteFileSize, err := getRemoteFileSize(c, remoteFilePath)
		if err != nil {
			log.Fatalf("Failed to get remote file size: %v", err)
		}

		if remoteFileSize <= offset {
			log.Println("No new data to download.")
			time.Sleep(4 * time.Second) // 暂停等待
			continue
		}

		// 从远程服务器下载增量数据
		bytesDownloaded, err := downloadIncrement(c, remoteFilePath, localFilePath, offset)
		if err != nil {
			log.Fatalf("Failed to download increment data from FTP server: %v", err)
		}

		if bytesDownloaded > 0 {
			fmt.Printf("Downloaded %d bytes (new offset: %d)\n", bytesDownloaded, offset+bytesDownloaded)

			// 更新偏移量
			newOffset := offset + bytesDownloaded
			if err := writeLastDownloadedOffset(offsetFilePath, newOffset); err != nil {
				log.Fatalf("Failed to update last downloaded offset: %v", err)
			}
			offset = newOffset // 更新偏移量
		}

		// 暂停等待下一次下载
		time.Sleep(4 * time.Second)
	}
}

func getRemoteFileSize(c *ftp.ServerConn, remotePath string) (int64, error) {
	// 使用 SIZE 命令获取远程文件大小
	response, err := c.FileSize(remotePath)
	if err != nil {
		return 0, err
	}
	return response, nil
}

func downloadIncrement(c *ftp.ServerConn, remotePath string, localFilePath string, offset int64) (int64, error) {
	// 打开目标文件以进行写入
	lFile, err := os.OpenFile(localFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer lFile.Close()

	// 从目标文件末尾开始
	tmpFile, err := os.CreateTemp("", "ftp_temp_")
	if err != nil {
		return 0, err
	}
	defer os.Remove(tmpFile.Name())

	resp, err := c.Retr(remotePath)
	if err != nil {
		return 0, err
	}
	defer resp.Close()

	// 先将之前存在的部分跳过
	if offset > 0 {
		if _, err := io.CopyN(io.Discard, resp, offset); err != nil && err != io.EOF {
			return 0, err
		}
	}

	// 将增量数据写入临时文件
	bytesRead, err := io.Copy(lFile, resp)
	if err != nil {
		return 0, err
	}

	return bytesRead, nil
}

// 读取上次下载的偏移量
func readLastDownloadedOffset(filePath string) (int64, error) {
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

// 写入新的下载偏移量
func writeLastDownloadedOffset(filePath string, offset int64) error {
	return os.WriteFile(filePath, []byte(fmt.Sprintf("%d", offset)), 0644)
}

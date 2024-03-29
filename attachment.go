package snip

import (
	"fmt"
	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"github.com/google/uuid"
	"github.com/ryanfrishkorn/snip/database"
	"strconv"
	"time"
)

// Attachment represents data (binary safe) associated with a specific snip
type Attachment struct {
	UUID      uuid.UUID
	Data      []byte
	Size      int
	SnipUUID  uuid.UUID
	Timestamp time.Time
	Name      string
}

// GetAttachmentMetadata returns all fields except Data for analysis without large memory use
func GetAttachmentMetadata(searchUUID uuid.UUID) (Attachment, error) {
	a := Attachment{}

	var stmt *sqlite3.Stmt
	stmt, err := database.Conn.Prepare(`SELECT size, snip_uuid, timestamp, name FROM snip_attachment WHERE uuid = ?`, searchUUID.String())
	if err != nil {
		return a, err
	}
	defer stmt.Close()

	err = stmt.Exec()
	if err != nil {
		return a, err
	}

	resultCount := 0
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return a, err
		}
		if !hasRow {
			break
		}
		resultCount++
		// enforce only one result to avoid ambiguous behavior
		if resultCount > 1 {
			return a, fmt.Errorf("database search returned multiple results")
		}

		var (
			size      string
			snipUUID  string
			timestamp string
			name      string
		)
		err = stmt.Scan(&size, &snipUUID, &timestamp, &name)
		if err != nil {
			return a, err
		}
		a.UUID = searchUUID
		if err != nil {
			return a, fmt.Errorf("error parsing uuid string into struct")
		}
		a.Size, err = strconv.Atoi(size)
		if err != nil {
			return a, err
		}
		a.Timestamp, err = time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			return a, err
		}
		a.Name = name
	}
	if resultCount == 0 {
		return a, fmt.Errorf("database search returned zero results")
	}
	return a, nil
}

func GetAttachmentFromUUID(searchUUID string) (Attachment, error) {
	a := Attachment{}

	searchUUIDFuzzy := "%" + searchUUID + "%"
	var stmt *sqlite3.Stmt
	stmt, err := database.Conn.Prepare(`SELECT uuid, data, name, size, snip_uuid, timestamp FROM snip_attachment WHERE uuid LIKE ?`, searchUUIDFuzzy)
	if err != nil {
		return a, err
	}
	defer stmt.Close()

	if err != nil {
		return a, err
	}

	resultCount := 0
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return a, err
		}
		if !hasRow {
			break
		}
		resultCount++
		// enforce only one result to avoid ambiguous behavior
		if resultCount > 1 {
			return a, fmt.Errorf("database search returned multiple results")
		}

		var (
			id        string
			data      string
			name      string
			size      string
			snipUUID  string
			timestamp string
		)
		err = stmt.Scan(&id, &data, &name, &size, &snipUUID, &timestamp)
		if err != nil {
			return a, err
		}
		a.UUID, err = uuid.Parse(id)
		if err != nil {
			return a, fmt.Errorf("error parsing uuid string into uuid type")
		}
		a.Data = []byte(data)
		a.Size, err = strconv.Atoi(size)
		if err != nil {
			return a, err
		}
		a.Timestamp, err = time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			return a, err
		}
		a.Name = name
	}
	if resultCount == 0 {
		return a, fmt.Errorf("database search returned zero results")
	}
	return a, nil
}

// NewAttachment returns a new attachment struct with current defaults
func NewAttachment() Attachment {
	return Attachment{
		Data:      []byte{},
		Size:      0,
		Timestamp: time.Now(),
		Name:      "",
		UUID:      uuid.New(),
	}
}

// RemoveAttachment deletes an attachment from the database
func RemoveAttachment(id uuid.UUID) error {
	// see if it exists first
	stmt, err := database.Conn.Prepare(`SELECT uuid FROM snip_attachment where uuid = ? LIMIT 2`, id.String())
	if err != nil {
		return err
	}

	count := 0
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			stmt.Close()
			return err
		}
		if !hasRow {
			break
		}
		count += 1
	}
	stmt.Close()

	// result should always be unique
	if count == 0 {
		return fmt.Errorf("could not locate attachment")
	}
	if count != 1 {
		return fmt.Errorf("attachment id returned ambiguous results")
	}

	// remove
	stmt, err = database.Conn.Prepare(`DELETE FROM snip_attachment WHERE uuid = ?`, id.String())
	if err != nil {
		return err
	}
	err = stmt.Exec()
	stmt.Close()
	if err != nil {
		return err
	}
	return nil
}

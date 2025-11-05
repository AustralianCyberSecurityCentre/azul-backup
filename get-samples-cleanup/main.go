package main

// Script to allow for the conversion to json from avro and to avro from json.
// This allows for modification of the backup models.

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/events"
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v9/gosrc/msginflight"
)

const filePerms = 0766

// Convert an Avro file into a json file
func AvroToJson(modelType events.Model, filepath string) {
	raw, err := os.ReadFile(filepath)
	if err != nil {
		log.Printf("Failed to load the json file %s", filepath)
		return
	}
	currentMsgs, err := msginflight.AvroBulkToMsgInFlights(raw, modelType)
	if err != nil {
		log.Printf("Failed to load avro file %s with error %v", filepath, err)
		return
	}
	rawJson, err := json.Marshal(currentMsgs)
	if err != nil {
		log.Printf("Failed to convert messages to Json %s with error %v", filepath, err)
		return
	}
	filepath = filepath + ".json"
	err = os.WriteFile(filepath, rawJson, filePerms)
	if err != nil {
		log.Printf("Failed to write json to file %s with error %v", filepath, err)
		return
	}
}

// Convert a json file into an Avro file
func JsonToAvro(modelType events.Model, filepath string) {

	raw, err := os.ReadFile(filepath)
	if err != nil {
		log.Printf("Failed to load the json file %s", filepath)
		return
	}
	rawEvents := []json.RawMessage{}
	err = json.Unmarshal(raw, &rawEvents)
	if err != nil {
		log.Printf("Failed to marshal json %v for file file %s", err, filepath)
	}

	events := []*msginflight.MsgInFlight{}
	for _, curRawEvent := range rawEvents {
		newEvent, err := msginflight.NewMsgInFlightFromJson(curRawEvent, modelType)
		if err != nil {
			log.Printf("Failed to marshal event %v for file file %s", err, filepath)
		}
		events = append(events, newEvent)
	}

	bulkRawAvro, err := msginflight.MsgInFlightsToAvroBulk(events, modelType)
	if err != nil {
		log.Printf("Failed to convert messages to Avro for file %s with error %v", filepath, err)
		return
	}
	filepath = strings.ReplaceAll(filepath, ".json", "")
	err = os.WriteFile(filepath, bulkRawAvro, filePerms)
	if err != nil {
		log.Printf("Failed to write Avro to file %s with error %v", filepath, err)
		return
	}
}

// Convert the current Avro to Json.
func main() {
	store := "./testdata/data/samples"
	AvroToJson(events.ModelBinary, filepath.Join(store, "samples-binary-extracted-100.avro"))
	AvroToJson(events.ModelBinary, filepath.Join(store, "samples-binary-sourced-100.avro"))
	AvroToJson(events.ModelDelete, filepath.Join(store, "system-delete-default-2.avro"))
	AvroToJson(events.ModelInsert, filepath.Join(store, "system-insert-default-100.avro"))
	AvroToJson(events.ModelBinary, filepath.Join(store, "testing-binary-sourced-100.avro"))
}

// Uncomment to convert Json to Avro.
// func main() {
// 	store := "./testdata/data/samples"
// 	JsonToAvro(events.ModelBinary, filepath.Join(store, "samples-binary-extracted-100.avro.json"))
// 	JsonToAvro(events.ModelBinary, filepath.Join(store, "samples-binary-sourced-100.avro.json"))
// 	JsonToAvro(events.ModelDelete, filepath.Join(store, "system-delete-default-2.avro.json"))
// 	JsonToAvro(events.ModelInsert, filepath.Join(store, "system-insert-default-100.avro.json"))
// 	JsonToAvro(events.ModelBinary, filepath.Join(store, "testing-binary-sourced-100.avro.json"))
// }

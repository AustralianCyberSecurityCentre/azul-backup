package common

import (
	"github.com/AustralianCyberSecurityCentre/azul-bedrock/v10/gosrc/events"
)

func GetModelAndAction(source, modelOrAction string) (events.Model, events.BinaryAction) {
	model := events.ModelBinary
	action := events.BinaryAction(modelOrAction)
	if source == "system" {
		model = events.Model(modelOrAction)
		action = events.BinaryAction("default")
	}
	return model, action
}

package auth

import (
	"encoding/json"

	"github.com/chubaofs/chubaofs/util/keystore"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/auth"
	"github.com/chubaofs/chubaofs/util/cryptoutil"
)

type API struct {
	ac *AuthClient
}

func (api *API) GetTicket(clientId string, clientKey string, serviceID string) (ticket *auth.Ticket, err error) {
	var (
		key      []byte
		ts       int64
		msgResp  proto.AuthGetTicketResp
		respData []byte
	)
	message := proto.AuthGetTicketReq{
		Type:      proto.MsgAuthTicketReq,
		ClientID:  clientId,
		ServiceID: serviceID,
	}
	if key, err = cryptoutil.Base64Decode(clientKey); err != nil {
		return
	}
	if message.Verifier, ts, err = cryptoutil.GenVerifier(key); err != nil {
		return
	}
	if respData, err = api.ac.request(clientId, clientKey, key, message, proto.ClientGetTicket, serviceID); err != nil {
		return
	}
	if err = json.Unmarshal(respData, &msgResp); err != nil {
		return
	}
	if err = proto.VerifyTicketRespComm(&msgResp, proto.MsgAuthTicketReq, clientId, serviceID, ts); err != nil {
		return
	}
	ticket = &auth.Ticket{
		ID:         clientId,
		SessionKey: cryptoutil.Base64Encode(msgResp.SessionKey.Key),
		ServiceID:  cryptoutil.Base64Encode(msgResp.SessionKey.Key),
		Ticket:     msgResp.Ticket,
	}
	return
}

func (api *API) AdminCreateKey(clientID, clientKey, userID, role string, caps []byte) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID:   userID,
		Role: role,
		Caps: caps,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthCreateKeyReq, proto.AdminCreateKey)
}

func (api *API) AdminDeleteKey(clientID, clientKey, userID string) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID: userID,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthDeleteKeyReq, proto.AdminDeleteKey)
}

func (api *API) AdminGetKey(clientID, clientKey, userID string) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID: userID,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthGetKeyReq, proto.AdminGetKey)
}

func (api *API) AdminAddCaps(clientID, clientKey, userID string, caps []byte) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID:   userID,
		Caps: caps,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthAddCapsReq, proto.AdminAddCaps)
}

func (api *API) AdminDeleteCaps(clientID, clientKey, userID string, caps []byte) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID:   userID,
		Caps: caps,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthDeleteCapsReq, proto.AdminDeleteCaps)
}

func (api *API) AdminGetCaps(clientID, clientKey, userID string) (res *keystore.KeyInfo, err error) {
	if api.ac.ticket == nil {
		if api.ac.ticket, err = api.GetTicket(clientID, clientKey, proto.AuthServiceID); err != nil {
			return
		}
	}
	keyInfo := &keystore.KeyInfo{
		ID: userID,
	}
	return api.ac.serveAdminRequest(clientID, clientKey, api.ac.ticket, keyInfo, proto.MsgAuthGetCapsReq, proto.AdminGetCaps)
}

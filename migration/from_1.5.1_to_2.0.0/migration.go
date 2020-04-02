// Copyright 2018 The ChubaoFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Compared with version 1.5.1, version 2.0.0 adds user mechanism and volume resource authorization function.
// The migration from version 1.5.1 to version 2.0.0 only requires the establishment of user data and authorization
// relationships, and does not require the migration of data and metadata.
//
// The volume list of the object storage interface is currently used.
// The following volumes require special handling.
// 		ads_data_algorithm_offline_task_fnfo618wqhkkt
// 		ads_serving_material_dump_6_zmrmngnmzdaxzdcxmdixzdmxnzg2otvi
// 		ads_xn
// 		ads_xn_osstest_zdljotu0ngyzntu3yza5mgmymtm5nwiy
// 		bpp_op_requests
// 		datahubcfs
// 		jdspark
// 		jrctest1
// 		offline_logs
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
)

var (
	specialVolumes = []string{
		"ads_data_algorithm_offline_task_fnfo618wqhkkt",
		"ads_serving_material_dump_6_zmrmngnmzdaxzdcxmdixzdmxnzg2otvi",
		"ads_xn",
		"ads_xn_osstest_zdljotu0ngyzntu3yza5mgmymtm5nwiy",
		"bpp_op_requests",
		"datahubcfs",
		"jdspark",
		"jrctest1",
		"offline_logs",
		"vol3",
		"vol2",
	}
)

func isSpecialVolume(volume string) bool {
	for _, specialVolume := range specialVolumes {
		if specialVolume == volume {
			return true
		}
	}
	return false
}

func main() {
	var err error

	var optMaster string
	var optLog string

	flag.StringVar(&optMaster, "master", "", "Master address or hostname of ChubaoFS cluster")
	flag.StringVar(&optLog, "log", "migration.log", "Log file path")
	flag.Parse()
	if optMaster == "" {
		flag.Usage()
		os.Exit(0)
	}

	// Init log
	var logFile *os.File
	if logFile, err = os.OpenFile(optLog, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "An error occurred during execution: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		_ = logFile.Sync()
		_ = logFile.Close()
	}()
	log.SetFlags(log.Ldate | log.Lshortfile | log.LstdFlags)
	log.SetOutput(logFile)

	fmt.Printf("ChubaoFS Migrate tool for v1.5.1 to v2.0.0\n")
	fmt.Printf("Using master address: %v\n", optMaster)
	fmt.Printf("Confirm to execute mgiration (yes/no)[no]: ")
	var confirm string
	_, _ = fmt.Scanln(&confirm)
	confirm = strings.TrimSpace(strings.ToLower(confirm))
	if confirm != "y" && confirm != "yes" {
		fmt.Printf("Abort by user.\n")
		os.Exit(0)
	}
	if err = execute(optMaster); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "An error occurred during execution: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func execute(address string) (err error) {
	client := master.NewMasterClient([]string{address}, false)

	// 1. Collect volume list
	fmt.Printf("Collecting volumes ... ")
	var clusterView *proto.ClusterView
	if clusterView, err = client.AdminAPI().GetCluster(); err != nil {
		log.Printf("Exeucte: get cluster info fail: err(%v)", err)
		statusFail()
		return
	}
	statusDone()
	var volumes = clusterView.VolStatInfo

	// 2. Process volumes one by one
	sort.SliceStable(volumes, func(i, j int) bool {
		// Prioritize special volumes
		isISpec := isSpecialVolume(volumes[i].Name)
		isJSpec := isSpecialVolume(volumes[j].Name)
		if (isISpec && isJSpec) || (!isISpec && !isJSpec) {
			return volumes[i].Name < volumes[j].Name
		}
		return isISpec && !isJSpec
	})
	for _, volumeInfo := range volumes {
		fmt.Printf("Process %v ... ", volumeInfo.Name)
		processRetry := 0
		for processRetry < 3 {
			if err = processVolume(volumeInfo.Name, client); err != nil {
				log.Printf("Execute: process volume fail: volume(%v) err(%v)", volumeInfo.Name, err)
				processRetry++
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break
		}
		if err != nil {
			statusFail()
			return
		}
		statusDone()

		fmt.Printf("Verify %v ... ", volumeInfo.Name)
		verifyRetry := 0
		for verifyRetry < 3 {
			if err = validateVolume(volumeInfo.Name, client); err != nil {
				log.Printf("Execute: verify volume fail: volume(%v) err(%v)", volumeInfo.Name, err)
				verifyRetry++
				time.Sleep(100 * time.Millisecond)
				continue
			}
			break
		}
		if err != nil {
			statusFail()
			return
		}
		statusDone()

	}
	return
}

func processVolume(volumeName string, client *master.MasterClient) (err error) {
	// Collect volume info
	var volView *proto.VolView
	if volView, err = client.ClientAPI().GetVolumeWithoutAuthKey(volumeName); err != nil {
		statusFail()
		return
	}
	owner := volView.Owner
	ossSecure := volView.OSSSecure

	var userInfo *proto.UserInfo
	// Check user exist
	userInfo, err = client.UserAPI().GetUserInfo(owner)

	if err == proto.ErrUserNotExists {
		// If the user does not exist, create a user to associate with it.
		if userInfo, err = client.UserAPI().CreateUser(&proto.UserCreateParam{
			ID:        owner,
			AccessKey: ossSecure.AccessKey,
			SecretKey: ossSecure.SecretKey,
			Type:      proto.UserTypeNormal}); err != nil {
			log.Printf("Process: create volume owner fail: volume(%v) user(%v) accessKey(%v) secretKey(%v) err(%v)",
				volumeName, owner, ossSecure.AccessKey, ossSecure.SecretKey, err)
			return
		}

	}
	if err != nil {
		// An unexpected error occurred.
		return
	}
	if !userInfo.Policy.IsOwn(volumeName) {
		// Transfer volume to user
		if _, err = client.UserAPI().TransferVol(&proto.UserTransferVolParam{
			Volume:  volumeName,
			UserDst: owner,
			Force:   true}); err != nil {
			log.Printf("[ERROR] Process: transfer volume fail: volume(%v) user(%v) err(%v)",
				volumeName, owner, err)
			return
		}
		log.Printf("[INFO] Process: transfer volume: volume(%v) user(%v)",
			volumeName, owner)
	}

	if ossSecure.AccessKey != userInfo.AccessKey && isSpecialVolume(volumeName) {
		// Create a shadow user and authorize it.
		var shadowUser string
		if len(volumeName) > 8 {
			shadowUser = owner + "_" + volumeName[:8]
		} else {
			shadowUser = owner + "_" + volumeName
		}
		shadowUser = strings.Trim(shadowUser, "_")
		log.Printf("Process: shadow user: volume(%v) owner(%v) shadow(%v)", volumeName, owner, shadowUser)
		_, err = client.UserAPI().CreateUser(&proto.UserCreateParam{
			ID:        shadowUser,
			AccessKey: ossSecure.AccessKey,
			SecretKey: ossSecure.SecretKey,
			Type:      proto.UserTypeNormal,
		})
		if err == proto.ErrDuplicateUserID {
			err = nil
		}
		if err != nil {
			return
		}

		if _, err = client.UserAPI().UpdatePolicy(&proto.UserPermUpdateParam{
			UserID: shadowUser,
			Volume: volumeName,
			Policy: []string{proto.BuiltinPermissionWritable.String()},
		}); err != nil {
			log.Printf("Process: setup shadow user permission fail: volume(%v) owner(%v) shadow(%v) err(%v)",
				volumeName, owner, shadowUser, err)
			return
		}

	}
	return
}

func validateVolume(volumeName string, client *master.MasterClient) (err error) {
	var volumeView *proto.VolView
	if volumeView, err = client.ClientAPI().GetVolumeWithoutAuthKey(volumeName); err != nil {
		return
	}
	var userInfo *proto.UserInfo
	if userInfo, err = client.UserAPI().GetUserInfo(volumeView.Owner); err != nil {
		log.Printf("Verify: get user info fail: volume(%v) owner(%v) err(%v)",
			volumeName, volumeView.Owner, err)
		return
	}
	if !userInfo.Policy.IsOwn(volumeName) {
		err = fmt.Errorf("owner mismatch")
		log.Printf("Verify: check owner mismatch: volume(%v) expect(%v) actual(%v) raw(%v)", volumeName, userInfo.UserID, volumeView.Owner, userInfo.Policy.OwnVols)
		return
	}
	if !isSpecialVolume(volumeView.Name) {
		return
	}

	if volumeView.OSSSecure == nil {
		return
	}
	if userInfo.AccessKey == volumeView.OSSSecure.AccessKey && userInfo.SecretKey == volumeView.OSSSecure.SecretKey {
		return
	}
	var shadowUser string
	if len(volumeView.Name) > 8 {
		shadowUser = volumeView.Owner + "_" + volumeView.Name[:8]
	} else {
		shadowUser = volumeView.Owner + "_" + volumeView.Name
	}
	shadowUser = strings.Trim(shadowUser, "_")
	if userInfo, err = client.UserAPI().GetUserInfo(shadowUser); err != nil {
		log.Printf("Verify: get shadow user fail: volume(%v) shadow(%v) err(%v)", volumeName, shadowUser, err)
		return
	}
	for _, action := range proto.BuiltinPermissionActions(proto.BuiltinPermissionWritable) {
		if !userInfo.Policy.IsAuthorized(volumeView.Name, action) {
			err = fmt.Errorf("permission mismatch")
			return
		}
	}
	return
}

func statusFail() {
	fmt.Printf("%v\n", redFont("fail"))
}

func statusDone() {
	fmt.Printf("%v\n", greenFont("done"))
}

func greenFont(msg string) string {
	return "\033[32m" + msg + "\033[0m"
}

func redFont(msg string) string {
	return "\033[31m" + msg + "\033[0m"
}

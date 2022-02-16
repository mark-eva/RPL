package manager

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Manager is a struct equivalent of the json file consumed

type Manager struct {
	History []struct {
		ID             int         `json:"id"`
		Points         int         `json:"points"`
		TotalPoints    int         `json:"total_points"`
		Rank           interface{} `json:"rank"`
		RankSort       interface{} `json:"rank_sort"`
		EventTransfers int         `json:"event_transfers"`
		PointsOnBench  int         `json:"points_on_bench"`
		Entry          int         `json:"entry"`
		Event          int         `json:"event"`
	} `json:"history"`
	Entry struct {
		EventPoints       int    `json:"event_points"`
		FavouriteTeam     int    `json:"favourite_team"`
		ID                int    `json:"id"`
		LeagueSet         []int  `json:"league_set"`
		Name              string `json:"name"`
		OverallPoints     int    `json:"overall_points"`
		PlayerFirstName   string `json:"player_first_name"`
		PlayerLastName    string `json:"player_last_name"`
		RegionName        string `json:"region_name"`
		RegionCodeShort   string `json:"region_code_short"`
		RegionCodeLong    string `json:"region_code_long"`
		StartedEvent      int    `json:"started_event"`
		TransactionsEvent int    `json:"transactions_event"`
		TransactionsTotal int    `json:"transactions_total"`
	} `json:"entry"`
}

// TruncateManager truncates the manager table in the table as part of refresh process before re-inserting the data
func TruncateTable(tablename string) {
	// Create a database object
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"

	// Initialise the db con cobject
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	deleteStatement := "truncate rpl." + tablename

	// Truncate Manager table
	_, err = db.Exec(deleteStatement)

	if err != nil {
		panic(err)
	}

}

func ManagerValid(ManagerID int) (bool, int) {

	// Convert the manager id from int to string
	managerID := strconv.Itoa(ManagerID)

	// Define the API URL
	managerURL := "https://draft.premierleague.com/api/entry/" + managerID + "/history"

	// Declares the default state of the boolean return value
	var ManagerValid bool = false
	var ManagerLeagueID int = 0

	// Invoke the Get Method
	response, err := http.Get(managerURL)

	// fmt.Println("HTTP Response Status:", response.StatusCode, http.StatusText(response.StatusCode))

	if err != nil || response.StatusCode != 200 {
		fmt.Printf("The HTTP request failed with error %s\n", err)
		fmt.Println("HTTP Response Status:", response.StatusCode)
		return ManagerValid, ManagerLeagueID

	} else {
		// Ready the body response
		body, _ := ioutil.ReadAll(response.Body)

		// Creates a new object of manager
		var manager Manager

		// Stores json data the new manager object
		err := json.Unmarshal([]byte(body), &manager)

		// If there is an error return false
		if err != nil {

			// Return false for manager validity and
			fmt.Println("Manager ID is not valid ", managerID)
			return ManagerValid, ManagerLeagueID

		} else {

			fmt.Println("HTTP Response Status:", response.StatusCode)
			fmt.Println("Manager's League ID is: ", manager.Entry.LeagueSet[0])
			ManagerValid := true
			return ManagerValid, manager.Entry.LeagueSet[0]
		}

	}

}

// Function that queries table rpl.league_manager_allocation

func GetManagerIDs() []int {

	// Declare a struct that will store the values of the db query results

	var (
		managerid int
	)

	// var managers Managers

	var managerIDs []int

	// Initialise the db con cobject
	// connStr := "user=goapiserviceuser dbname=goapi password=oracle host=192.168.56.101 sslmode=disable"
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	// Build the select statement
	selectStatement := "select distinct entryid as managerid from rpl.league_manager_allocation"

	// Execute the select statement
	rows, err := db.Query(selectStatement)

	// var managerIDSlice []Managers

	for rows.Next() {

		err := rows.Scan(&managerid)
		if err != nil {
			log.Fatal(err)
		}

		managerIDs = append(managerIDs, managerid)

	}

	return managerIDs

}

// InsertLatestInfo retuns nothing
func InsertLatestInfo(managerIDs []int) {

	fmt.Println("------------------Fetching Manager Data------------------")

	// managerIDs := GetManagerIDs()

	for _, e := range managerIDs {

		// Convert integer to string
		managerid := strconv.Itoa(e)

		managerURL := "https://draft.premierleague.com/api/entry/" + managerid + "/history"

		// Iterate over list of manager id to gather data

		response, err := http.Get(managerURL)

		if err != nil {
			fmt.Printf("The HTTP request failed with error %s\n", err)
		} else {
			body, _ := ioutil.ReadAll(response.Body)

			// Creates a new object of manager
			var manager Manager

			err := json.Unmarshal([]byte(body), &manager)
			if err != nil {
				panic(err)
			}

			// Converts managers overall points from int to string
			// OverallPoints := strconv.Itoa(manager.Entry.OverallPoints)

			// Determines which league the manager belongs to
			leagueid := manager.Entry.LeagueSet[0]

			// Convert integer to string
			leagueidstring := strconv.Itoa(leagueid)

			// Build the select statement to find out the league name
			leagueNameQuery := "select name from rpl.league where id = " + leagueidstring

			// Invoke the query

			fmt.Println("-------------------------------------------------")
			fmt.Println("First Name: ", manager.Entry.PlayerFirstName)
			fmt.Println("Second Name: ", manager.Entry.PlayerLastName)

			// Create a database object

			// Initialise the db con cobject
			// connStr := "user=goapiserviceuser dbname=goapi password=oracle host=192.168.56.101 sslmode=disable"
			connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
			db, err := sql.Open("postgres", connStr)

			// Check for error and panic is there is one
			if err != nil {
				panic(err)
			}
			defer db.Close()

			err = db.Ping()
			if err != nil {
				panic(err)
			}

			// Declare the league name variable
			var leagueName string

			// Execute the sql command
			err = db.QueryRow(leagueNameQuery).Scan(&leagueName)
			if err != nil {
				log.Println(err)
			}

			// Build the insert query
			insertStatement := `
			INSERT INTO rpl.manager (ID
			, League
			, Name
			, PlayerFirstName
			, PlayerLastName
			, OverallPoints
			, CurrentGWPoints
			, LeagueSet  
			, TransactionsEvent 
			, TransactionsTotal)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

			// Execute the insert statement
			_, err = db.Exec(
				insertStatement,
				manager.Entry.ID,
				leagueName,
				manager.Entry.Name,
				manager.Entry.PlayerFirstName,
				manager.Entry.PlayerLastName,
				manager.Entry.OverallPoints,
				manager.Entry.EventPoints,
				manager.Entry.LeagueSet[0],
				manager.Entry.TransactionsEvent,
				manager.Entry.TransactionsTotal,
			)

			if err != nil {
				panic(err.Error())
			}

			if err != nil {
				panic(err)
			}

		}

		if err != nil {
			log.Fatal(err)
		}
	}
	fmt.Println("-------------------------------------------------")
	fmt.Println("------------------------Inserting on managerhistory-------------------------")

	// Check is a missing gameweek for each managerids in the managerhistory table
	missingGameweekdata, managerlist := IsThereGapOnGameweek("managerhistory")
	fmt.Println("Missing data in managerhistory", missingGameweekdata)

	// Invoke Insert Manager history data
	InsertManagerHistory()

	if missingGameweekdata {
		fmt.Println("Missing data in managerhistory", missingGameweekdata)
		fmt.Println("List of manager ids with missing data", managerlist)
		// Inserting
		InsertManagerHistory()

	} else {
		fmt.Println("No missing data skipping managerhistory refresh..")
	}

}

func ReturnCombinedGameweek() [][]string {

	type LeagueTableInfo struct {
		Rank    string
		League  string
		Name    string
		Manager string
		GW      string
		PTS     string
		DIF     string
		MP      string
		GS      string
		A       string
		CS      string
		BESTGW  string
		WORSTGW string
		AVGGW   string
	}

	fmt.Println("Endpoint Hit: Combined leaague table")

	// Initialise the db con cobject
	// connStr := "user=goapiserviceuser dbname=goapi password=oracle host=192.168.56.101 sslmode=disable"
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	db, err := sql.Open("postgres", connStr)

	// Error checking/handling
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	// Build the select statement
	CombinedLeague := `
	select base."POS."
	, base."L."
	, base."TEAM NAME"
	, base."MANAGER"
	, base."GW"
	, base."PTS"
	, COALESCE (base."PTS" - lag (base."PTS") over (order by base."PTS" desc, base."GW"), 0) as "DIF"
	, playerstats."MP"
	, playerstats."GS"
	, playerstats."A"
	, playerstats."CS"
	, bestgw."BEST GW"
	, bestgw."WORST GW"
	, bestgw."AVG GW"
	from (
	select a.id
		  , RANK () OVER (order by a.overallpoints desc ) as "POS."
		  , a.league as "L."
		  , upper (a.name) as "TEAM NAME"
		  , upper (a.playerfirstname || ' ' || SUBSTRING (a.playerlastname, 1, 1) )  as "MANAGER"
		  , a.currentgwpoints as "GW"
		  , a.overallpoints as "PTS"
		from rpl.manager a, rpl.managerhistory b
		where a.id = b.managerid
		and b.gameweek = (select max(gameweek) from rpl.managerhistory)
		order by overallpoints desc
	) as base
	, 
	( 	select   sum (minutes) as "MP" 
		 , sum (goalsscored) as "GS"
		 , sum (assists) as "A"
		 , sum (cleansheets) as "CS"
		 , managerid 
		from (	select a.id as "playerid"
		, a.gameweek
		, a.assists
		, a.minutes
		, a.goalsscored
		, a.totalpoints
		, a.cleansheets
		, b.managerid
		from rpl.playerstats a, rpl.playerallocation b
		where  a.id = b.playerid  
		and a.gameweek = b.gameweeknumber
		and id in (select distinct playerid from rpl.playerallocation)
	) as gs
	group by managerid) as playerstats
	, 
	( select max (points) as "BEST GW"
		, min (points) as "WORST GW"
		, ROUND(AVG(points)::numeric,3)     as "AVG GW"
		, managerid 
	from rpl.managerhistory
	group by managerid
	order by 2 desc) as bestgw
	where playerstats.managerid = base.id
	and bestgw.managerid = base.id`

	// Execute the select statement
	rows, err := db.Query(CombinedLeague)
	cols, err := rows.Columns()

	// Declare the mapping between variable and struct
	var leaguetableinfo LeagueTableInfo

	// Declare the league table slice
	var leaguetableSlice []LeagueTableInfo
	var matrix [][]string
	matrix = append(matrix, []string{cols[0],
		cols[1],
		cols[2],
		cols[3],
		cols[4],
		cols[5],
		cols[6],
		cols[7],
		cols[8],
		cols[9],
		cols[10],
		cols[11],
		cols[12],
		cols[13],
	})

	for rows.Next() {
		rows.Scan(&leaguetableinfo.Rank, &leaguetableinfo.League, &leaguetableinfo.Name,
			&leaguetableinfo.Manager, &leaguetableinfo.GW, &leaguetableinfo.PTS, &leaguetableinfo.DIF,
			&leaguetableinfo.MP, &leaguetableinfo.GS, &leaguetableinfo.A, &leaguetableinfo.CS, &leaguetableinfo.BESTGW,
			&leaguetableinfo.WORSTGW, &leaguetableinfo.AVGGW)

		leaguetableSlice = append(leaguetableSlice, leaguetableinfo)

		// Create a multi-dimensional array
		matrix = append(matrix, []string{leaguetableinfo.Rank,
			leaguetableinfo.League,
			leaguetableinfo.Name,
			leaguetableinfo.Manager,
			leaguetableinfo.GW,
			leaguetableinfo.PTS,
			leaguetableinfo.DIF,
			leaguetableinfo.MP,
			leaguetableinfo.GS,
			leaguetableinfo.A,
			leaguetableinfo.CS,
			leaguetableinfo.BESTGW,
			leaguetableinfo.WORSTGW,
			leaguetableinfo.AVGGW,
		})

	}

	// Create a multi dimensional array from leaguetableslice
	for i, s := range leaguetableSlice {
		fmt.Println(i, s)
		// toReturn = append(toReturn, data)

	}

	// fmt.Println("Combined league table")
	fmt.Println(cols[1])
	// fmt.Println(leaguetableSlice)
	fmt.Println("--------")
	fmt.Println(matrix)
	fmt.Println("--------")

	return matrix

}

func RefreshData() {

	// Truncate and insert and insert the latest manager data
	TruncateTable("manager")
	TruncateTable("playerallocation")
	TruncateTable("playerstats")

	// Insert latest info
	managerIDs := GetManagerIDs()
	InsertLatestInfo(managerIDs)

	// Insert player allocation
	InsertPlayerAllocation()

	InsertUpto := InsertUpGW()

	var playerStatsURL string = "https://fantasy.premierleague.com/api/event/"
	var playerFileName string = "playerstatsgw_"
	var playerFileNameSuffix string = "_.json"

	for gw := 1; gw <= InsertUpto; gw++ {
		gwstring := strconv.Itoa(gw)
		dlPlayerStatsURL := playerStatsURL + gwstring + "/live"
		dlFilename := playerFileName + gwstring + playerFileNameSuffix

		playerFileDir := DownloadJsonFile(dlPlayerStatsURL, dlFilename)
		InsertPlayerStats(playerFileDir, gwstring)

	}

}

func DataRefresh() {
	// Always truncate and refresh data whenever the application is started
	RefreshData()
	time.Sleep(900 * time.Second)

	for {

		// Declare a bolean for testing
		// var CurrentGWStarted bool = true
		CurrentGWInProgress := GWInProgress()
		fmt.Println("Current gameweek in progress: ", CurrentGWInProgress)

		// Detect if the current gameweek has started
		log.Printf("-----------------------------------------------")
		_, Currentgw, _ := GetLatestGW()

		// fmt.Println("Last completed Gameweek is  :", Completedgw)
		// fmt.Println("Game week in progress  :", Currentgw)
		// fmt.Println("Current gameweek", Currentgw, "is in progress:", CurrentGWInProgress)

		if !CurrentGWInProgress {
			// Sleep for 15 minutes if it hasnt gameweek is not in progress
			log.Printf("-----------------------------------------------")
			fmt.Println("Gameweek", Currentgw, "has not started yet")
			time.Sleep(900 * time.Second)

		} else {
			log.Printf("-----------------------------------------------")
			fmt.Println("Gameweek", Currentgw, "started refreshing data 15 minute...")
			time.Sleep(900 * time.Second)
			RefreshData()
		}
	}

}

type LeagueInfo struct {
	League struct {
		AdminEntry         int       `json:"admin_entry"`
		Closed             bool      `json:"closed"`
		DraftDt            time.Time `json:"draft_dt"`
		DraftPickTimeLimit int       `json:"draft_pick_time_limit"`
		DraftStatus        string    `json:"draft_status"`
		DraftTzShow        string    `json:"draft_tz_show"`
		ID                 int       `json:"id"`
		KoRounds           int       `json:"ko_rounds"`
		MakeCodePublic     bool      `json:"make_code_public"`
		MaxEntries         int       `json:"max_entries"`
		MinEntries         int       `json:"min_entries"`
		Name               string    `json:"name"`
		Scoring            string    `json:"scoring"`
		StartEvent         int       `json:"start_event"`
		StopEvent          int       `json:"stop_event"`
		Trades             string    `json:"trades"`
		TransactionMode    string    `json:"transaction_mode"`
		Variety            string    `json:"variety"`
	} `json:"league"`
	LeagueEntries []struct {
		EntryID         int       `json:"entry_id"`
		EntryName       string    `json:"entry_name"`
		ID              int       `json:"id"`
		JoinedTime      time.Time `json:"joined_time"`
		PlayerFirstName string    `json:"player_first_name"`
		PlayerLastName  string    `json:"player_last_name"`
		ShortName       string    `json:"short_name"`
		WaiverPick      int       `json:"waiver_pick"`
	} `json:"league_entries"`
	Standings []struct {
		EventTotal  int `json:"event_total"`
		LastRank    int `json:"last_rank"`
		LeagueEntry int `json:"league_entry"`
		Rank        int `json:"rank"`
		RankSort    int `json:"rank_sort"`
		Total       int `json:"total"`
	} `json:"standings"`
}

func GetAllManagerIDFromLeagueID(leagueid int) []int {
	fmt.Println("------------------Extracting all manager id in league: ", leagueid, " --------------")

	// Creates a new object of Leage info originating from struct
	var leagueInfo LeagueInfo

	// Convert the leagueid to string
	leagueId := strconv.Itoa(leagueid)

	// Download json for league info
	leagueURL := "https://draft.premierleague.com/api/league/" + leagueId + "/details"
	leagueInfoFile := "leagueinfo_" + leagueId + ".json"
	fullPathofJsonFile := DownloadJsonFile(leagueURL, leagueInfoFile)

	// Probes the downloaded json file
	jsonFile, err := os.Open(fullPathofJsonFile)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	// read our opened jsonFile as a byte array.
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &leagueInfo)

	// Print Output for clearer logging
	fmt.Println("League Name: ", leagueInfo.League.Name)
	fmt.Println("League ID: ", leagueInfo.League.ID)

	// Declare and empty slice that will hold the returned data
	var allManagerIDs []int

	//Loop through the LeagueEntries
	for i, _ := range leagueInfo.LeagueEntries {
		// Assign each struct value into a variable
		entryid := leagueInfo.LeagueEntries[i].EntryID

		//Append all manager id to a slice that will eventually be returned
		fmt.Println(entryid)
		allManagerIDs = append(allManagerIDs, entryid)

	}

	return allManagerIDs

}

func InsertLeagueInfo(Leagueid int) []int {

	fmt.Println("------------------Inserting League Information------------------")

	// connStr := "user=goapiserviceuser dbname=goapi password=oracle host=192.168.56.101 sslmode=disable"
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	// Initialise the db con cobject
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Creates a new object of Leage info originating from struct
	var leagueInfo LeagueInfo
	var newValidManagerIDs []int

	// Convert the leagueid to string
	leagueId := strconv.Itoa(Leagueid)

	// Download json for league info
	leagueURL := "https://draft.premierleague.com/api/league/" + leagueId + "/details"
	leagueInfoFile := "leagueinfo_" + leagueId + ".json"
	fullPathofJsonFile := DownloadJsonFile(leagueURL, leagueInfoFile)

	// Probes the downloaded json file
	jsonFile, err := os.Open(fullPathofJsonFile)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	// read our opened jsonFile as a byte array.
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &leagueInfo)

	// Test that you can print out the data
	fmt.Println("League Name: ", leagueInfo.League.Name)
	fmt.Println("League ID: ", leagueInfo.League.ID)

	// Check if the league id already exist in the database
	countStatement := "select count (*) from rpl.league where id = " + leagueId

	// Declare row variable
	var rowCount int

	// Execute the sql command
	err = db.QueryRow(countStatement).Scan(&rowCount)
	if err != nil {
		log.Println(err)
	}

	fmt.Println("League id exist: ", rowCount)

	// Insert league id if it does not exist in the database

	if rowCount == 1 {
		log.Println("League ID: ", leagueId, " already exist. Insert not required")

	} else {

		// Create the syntax for insert statement
		insertOnLeague := `INSERT INTO rpl.league (adminentry
		,closed
		,draftdt
		,draftpicktimelimit
		,draftstatus
		,drafttzshow
		,id
		,korounds
		,makecodepublic
		,maxentries
		,minentries
		,name
		,scoring
		,startevent
		,stopevent
		,trades
		,transactionmode)
		VALUES  ($1, $2, $3, $4 , $5 , $6 , $7 , $8, $9
		, $10, $11 , $12 , $13 , $14 , $15, $16, $17)`

		// Insert the data into the database
		_, err = db.Exec(insertOnLeague,
			leagueInfo.League.AdminEntry,
			leagueInfo.League.Closed,
			leagueInfo.League.DraftDt,
			leagueInfo.League.DraftPickTimeLimit,
			leagueInfo.League.DraftStatus,
			leagueInfo.League.DraftTzShow,
			leagueInfo.League.ID,
			leagueInfo.League.KoRounds,
			leagueInfo.League.MakeCodePublic,
			leagueInfo.League.MaxEntries,
			leagueInfo.League.MinEntries,
			leagueInfo.League.Name,
			leagueInfo.League.Scoring,
			leagueInfo.League.StartEvent,
			leagueInfo.League.StopEvent,
			leagueInfo.League.Trades,
			leagueInfo.League.TransactionMode,
		)
		// Standard error checking
		if err != nil {
			panic(err)
		}
		// Commin transaction
		commitStatement := "commit"
		_, err = db.Exec(commitStatement)

		// Error check the commit statement
		if err != nil {
			panic(err)
		}

		// Insert each manager entries
		for i, _ := range leagueInfo.LeagueEntries {

			// Create the syntax of insert statement
			insertOnLeagueManagerAllocation := `
			INSERT INTO rpl.league_manager_allocation (entryid
			, entryname
			, id
			, joinedtime
			, playerfirstname
			, playerlastname
			, shortname
			, waiverpick
			, leagueid)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

			// Assign each struct value into a variable
			entryid := leagueInfo.LeagueEntries[i].EntryID
			entryname := leagueInfo.LeagueEntries[i].EntryName
			managerid := leagueInfo.LeagueEntries[i].ID
			joinedtime := leagueInfo.LeagueEntries[i].JoinedTime
			playerfirstname := leagueInfo.LeagueEntries[i].PlayerFirstName
			playerlastname := leagueInfo.LeagueEntries[i].PlayerLastName
			shortname := leagueInfo.LeagueEntries[i].ShortName
			waiverpick := leagueInfo.LeagueEntries[i].WaiverPick

			// Test print
			fmt.Println("Manager ID to be added in the Database: ", entryid)

			//Add the new manager id into a slice

			newValidManagerIDs = append(newValidManagerIDs, entryid)

			// Insert the data into the database
			_, err = db.Exec(insertOnLeagueManagerAllocation,
				entryid,
				entryname,
				managerid,
				joinedtime,
				playerfirstname,
				playerlastname,
				shortname,
				waiverpick,
				leagueId,
			)

			// Error check the the insert statement
			if err != nil {
				panic(err)
			}

		}
	}

	return newValidManagerIDs

}

func ManagerIdRegistered(ManagerId int) bool {

	// connStr := "user=goapiserviceuser dbname=goapi password=oracle host=192.168.56.101 sslmode=disable"
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	// Initialise the db con cobject
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Convert the leagueid to string
	managerId := strconv.Itoa(ManagerId)

	// Check if the league id already exist in the database
	countStatement := "select count (*) from rpl.league_manager_allocation where entryid = " + managerId

	// Declare row variable
	var managerIDCount int

	// Execute the sql command
	err = db.QueryRow(countStatement).Scan(&managerIDCount)
	if err != nil {
		log.Println(err)
	}

	if managerIDCount >= 1 {
		// log.Println("Manager ID exist: ", managerIDCount, " already exist. Insert not required")
		return true
	} else {
		// log.Println("Manager does not exist in the system.")
		return false
	}

}

func LeagueIdRegistered(LeagueId int) (bool, int) {

	// connStr := "user=goapiserviceuser dbname=goapi password=oracle host=192.168.56.101 sslmode=disable"
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	// Initialise the db con cobject
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Convert the leagueid to string
	leagueId := strconv.Itoa(LeagueId)

	// Check if the league id already exist in the database
	countStatement := "select count (*) from rpl.league where id =  " + leagueId
	playerCountStatement := "select count (distinct (entryid)) from rpl.league_manager_allocation where leagueid = " + leagueId

	// Declare row variable
	var leagueIDCount int
	var playerCount int

	// Execute the sql command
	db.QueryRow(countStatement).Scan(&leagueIDCount)
	db.QueryRow(playerCountStatement).Scan(&playerCount)
	if err != nil {
		log.Println(err)
	}

	if leagueIDCount >= 1 {
		log.Println("League ID exist. With ", playerCount, "registered players")
		return true, playerCount

	} else {
		log.Println("League ID does not exist in the system.")
		return false, playerCount

	}

}

func IsThereGapOnGameweek(tablename string) (bool, []int) {

	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	// fmt.Println("tablename is:", tablename)

	//Find out the latest gw details
	Completedgw, Currentgw, _ := GetLatestGW()
	CurrentGWInProgress := GWInProgress()

	var myGameweek string
	var rowCount int
	var managerid int
	var managerIDs []int

	// Determines up to which gameweek should be probed in terms of the gap
	if CurrentGWInProgress {
		fmt.Println("------------------------------------------------------------------")
		fmt.Println("Checking data gap until gameweek : ", Currentgw)
		myGameweek = strconv.Itoa(Currentgw)
		fmt.Println("------------------------------------------------------------------")

	} else {
		fmt.Println("------------------------------------------------------------------")
		fmt.Println("Checking data gap until gameweek : ", Completedgw)
		fmt.Println("------------------------------------------------------------------")
		myGameweek = strconv.Itoa(Completedgw)
	}

	// Build sql statement to check if the

	// Build the select query that checks if any manager gameweek details does not match with the number of gameweek
	selectStatement := `select count (*) from (select  distinct managerid, count (gameweek) gameweek_total 
	from rpl.` + tablename + ` group by managerid order by 2 ) as gameweek_total_per_manager
	where gameweek_total <` + myGameweek

	// Build the sql statment that outputs manager id with gap
	selectStatementManagerID := `select distinct managerid  from (select  distinct managerid, count (gameweek) gameweek_total 
	from rpl.` + tablename + ` group by managerid order by 2 ) as gameweek_total_per_manager
	where gameweek_total <` + myGameweek

	// Execute the sql command
	err = db.QueryRow(selectStatement).Scan(&rowCount)
	if err != nil {
		log.Println(err)
	}

	if rowCount >= 1 {

		// Executes query to find manager ids with missing data
		rows, err := db.Query(selectStatementManagerID)

		if err != nil {
			log.Fatal(err)
		}

		for rows.Next() {

			err := rows.Scan(&managerid)
			if err != nil {
				log.Fatal(err)
			}

			managerIDs = append(managerIDs, managerid)

		}

		return true, managerIDs

	} else {
		return false, managerIDs
	}

}

func GetMissingGameweekdata(tablename string, managerid int) []int {

	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	db, err := sql.Open("postgres", connStr)

	// Declare the slice that stores the missing gameweek
	var missingGW []int
	var missingGameweekNumber int

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	managerID := strconv.Itoa(managerid)

	// fmt.Println("tablename is:", tablename)
	// fmt.Println("managerid is:", managerid)

	//Find out the latest gw details
	Completedgw, Currentgw, _ := GetLatestGW()
	CurrentGWInProgress := GWInProgress()

	var myGameweek string

	// Determines up to which gameweek should be probed in terms of the gap
	if CurrentGWInProgress {
		// fmt.Println("-----------------------------------------------------")
		// fmt.Println("Probing details up to gameweek: ", Currentgw)
		myGameweek = strconv.Itoa(Currentgw)

	} else {
		// fmt.Println("-----------------------------------------------------")
		// fmt.Println("Probing details up to gameweek: ", Completedgw)
		myGameweek = strconv.Itoa(Completedgw)
	}

	// Build the sql statement
	selectStatement := `SELECT generate_series missing_gameweek FROM 
	generate_series(1, ` + myGameweek + `) WHERE NOT generate_series IN (
	SELECT gameweek FROM rpl.` + tablename + ` where managerid = ` + managerID + ")"

	// Executes query to find manager ids with missing data
	rows, err := db.Query(selectStatement)

	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {

		err := rows.Scan(&missingGameweekNumber)
		if err != nil {
			log.Fatal(err)
		}

		missingGW = append(missingGW, missingGameweekNumber)

	}

	return missingGW

}

func IsTableEmpty(tablename string) bool {

	// Create database connection
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	// Define the variable that will hold the result
	var rowCount int

	// Build the select statement
	selectStatement := "select count (*) from rpl." + tablename

	// Execute the sql command
	err = db.QueryRow(selectStatement).Scan(&rowCount)
	if err != nil {
		log.Println(err)
	}

	if rowCount == 0 {
		fmt.Println("Table name: ", tablename, "is currently empty")
		return true
	} else {

		fmt.Println("Table name: ", tablename, "has rows of:", rowCount)
		return false

	}

}

func InsertManagerHistory() {
	fmt.Println("------------------Inserting Manager History Data------------------")

	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	// Check if the table is empty
	tableEmpty := IsTableEmpty("managerhistory")

	// Get gameweek information
	_, currentgw, _ := GetLatestGW()
	currentgwinprogress := GWInProgress()

	fmt.Println("Current gameweek ", currentgw, "is in progress: ", currentgwinprogress)
	fmt.Println("is table empty", tableEmpty)

	// Refresh rpl.managerhistory data if the table is empty or if the currentgameweek is in progress
	if tableEmpty || currentgwinprogress {

		// Get all registered managerIDs
		managerIDs := GetManagerIDs()

		// Loop through each managerid
		for _, e := range managerIDs {

			// Convert integer to string
			managerid := strconv.Itoa(e)

			//Define the URL required for the subsequent API call
			managerURL := "https://draft.premierleague.com/api/entry/" + managerid + "/history"
			response, err := http.Get(managerURL)

			// Standard Error checking
			if err != nil {
				fmt.Printf("The HTTP request failed with error %s\n", err)
			} else {
				// IF there is no error then read the body response and save it to body variable
				body, _ := ioutil.ReadAll(response.Body)

				// Creates a new object of manager
				var manager Manager

				// Unmarshall the body of json file to the new manager object variable
				err := json.Unmarshal([]byte(body), &manager)
				if err != nil {
					panic(err)
				}

				fmt.Println("Inserting history data for manager id: ", managerid)

				// Iterate over all gameweek data per player

				for i, _ := range manager.History {

					// Create the syntax of insert statement
					insertOnManagerHistory := `
					INSERT INTO rpl.managerhistory (ID
					,Points
					,TotalPoints
					,EventTransfers
					,PointsOnBench
					,ManagerID
					,GameWeek)
					VALUES ($1, $2, $3, $4, $5, $6, $7)`
					// Assigne each instance to a variable
					historyid := manager.History[i].ID
					points := manager.History[i].Points
					totalPoints := manager.History[i].TotalPoints
					evenTransfers := manager.History[i].EventTransfers
					pointsOnBench := manager.History[i].PointsOnBench
					managerID := manager.History[i].Entry
					gameWeek := manager.History[i].Event
					// Insert the data into the database
					_, err = db.Exec(insertOnManagerHistory,
						historyid,
						points,
						totalPoints,
						evenTransfers,
						pointsOnBench,
						managerID,
						gameWeek,
					)
				}

			}

		}

	} else {
		fmt.Println("Table: rpl.managerhistory is not empty checking if there are any data gaps")

		// Check if there is any data gap in rpl.managerhistory
		gap, managerlist := IsThereGapOnGameweek("managerhistory")
		fmt.Println("Table rpl.managerhistory gap status: ", gap)

		// Get the managerids if there are any data gap in the table
		if gap {
			fmt.Println("-----Data gap exist on the following manager ids:----")

			// Loop through each manager and get the missig gameweek
			for _, value := range managerlist {
				fmt.Println("Check missing gameweek data for manager id: ", value)

				// Convert integer to string
				managerid := strconv.Itoa(value)

				// Consume API data per manager id with missing gameweek
				managerURL := "https://draft.premierleague.com/api/entry/" + managerid + "/history"
				response, err := http.Get(managerURL)

				// Standard error checking
				if err != nil {
					fmt.Printf("The HTTP request failed with error %s\n", err)
				} else {
					// IF there is no error then read the body response and save it to body variable
					body, _ := ioutil.ReadAll(response.Body)

					// Creates a new object of manager
					var manager Manager

					// Unmarshall the body of json file to the new manager object variable
					err := json.Unmarshal([]byte(body), &manager)
					if err != nil {
						panic(err)
					}

					// Get the missing gameweek per manager id
					missingGW := GetMissingGameweekdata("managerhistory", value)

					for _, gameweek := range missingGW {
						fmt.Println("Missing gameweek", gameweek)

						// Translate the index that needs to be used for each missing gameweek
						indexForInsert := gameweek - 1

						// Create the syntax of insert statement
						insertOnManagerHistory := `
						INSERT INTO rpl.managerhistory (ID
						,Points
						,TotalPoints
						,EventTransfers
						,PointsOnBench
						,ManagerID
						,GameWeek)
						VALUES ($1, $2, $3, $4, $5, $6, $7)`
						// Assigne each instance to a variable
						historyid := manager.History[indexForInsert].ID
						points := manager.History[indexForInsert].Points
						totalPoints := manager.History[indexForInsert].TotalPoints
						evenTransfers := manager.History[indexForInsert].EventTransfers
						pointsOnBench := manager.History[indexForInsert].PointsOnBench
						managerID := manager.History[indexForInsert].Entry
						gameWeek := manager.History[indexForInsert].Event
						// Insert the data into the database
						_, err = db.Exec(insertOnManagerHistory,
							historyid,
							points,
							totalPoints,
							evenTransfers,
							pointsOnBench,
							managerID,
							gameWeek,
						)
					}

				}

				fmt.Println("------------------------------------------------------------------")
			}

		} else {
			fmt.Println("No missing data in rpl.managerhistory... Nothing to see here")
			fmt.Println("------------------------------------------------------------------")
		}

	}
}

func InsertUpGW() int {
	fmt.Println("----------------------Game Week Details------------------")

	//Find out the latest gw details
	Completedgw, Currentgw, CurrentGWStarted := GetLatestGW()

	// Print gameweek details
	fmt.Println("Last completed Gameweek is  :", Completedgw)
	fmt.Println("Game week in progress  :", Currentgw)
	fmt.Println("Current gameweek has started :", CurrentGWStarted)

	fmt.Println("---------------------------------------------------------")

	var InsertUpto int = 0
	if !CurrentGWStarted {

		InsertUpto = Completedgw
		// fmt.Println("Data should be inserted up to gameweek : ", InsertUpto)

	} else {
		InsertUpto = Currentgw
		// fmt.Println("Data should be inserted up to gameweek : ", InsertUpto)

	}

	return InsertUpto

}

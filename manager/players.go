package manager

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"
)

type GameweekDetails []struct {
	Code                 int       `json:"code"`
	Event                int       `json:"event"`
	Finished             bool      `json:"finished"`
	FinishedProvisional  bool      `json:"finished_provisional"`
	ID                   int       `json:"id"`
	KickoffTime          time.Time `json:"kickoff_time"`
	Minutes              int       `json:"minutes"`
	ProvisionalStartTime bool      `json:"provisional_start_time"`
	Started              bool      `json:"started"`
	TeamA                int       `json:"team_a"`
	TeamAScore           int       `json:"team_a_score"`
	TeamH                int       `json:"team_h"`
	TeamHScore           int       `json:"team_h_score"`
	Stats                []struct {
		Identifier string        `json:"identifier"`
		A          []interface{} `json:"a"`
		H          []struct {
			Value   int `json:"value"`
			Element int `json:"element"`
		} `json:"h"`
	} `json:"stats"`
	TeamHDifficulty int `json:"team_h_difficulty"`
	TeamADifficulty int `json:"team_a_difficulty"`
	PulseID         int `json:"pulse_id"`
}

// we initialize our Users array
var gameweekDetails GameweekDetails
var Completedgw int = 0
var Currentgw int = 0
var CurrentGWStarted = true

// GetLatestGW returns the current gameweek
func GetLatestGW() (int, int, bool) {

	fmt.Println("Probing the following files to find out the latest gameweek info")
	fmt.Println("------------------------------------------------------------------")

	// Declare and empty variable that will hold the location of the file
	var fullPathofJsonFile string

	// Download all json files corresponding to 38 gamweeks
OuterLoop:
	for gw := 1; gw < 39; gw++ {

		// convert gameweek int to string
		gwstring := strconv.Itoa(gw)

		// Default file name
		fullPathofJsonFile = "/tmp/fixturedetailsgw_" + gwstring + ".json"

		// Check if the fixture file already exist
		if _, err := os.Stat(fullPathofJsonFile); err == nil {

			// THe file does exist so we can probe the json file without downloading it first
			fmt.Println("File", fullPathofJsonFile, "exists skipping download")

		} else {

			fixtureDetailsUrl := "https://fantasy.premierleague.com/api/fixtures?event=" + gwstring
			fixturesFilename := "fixturedetailsgw_" + gwstring + ".json"
			fullPathofJsonFile := DownloadJsonFile(fixtureDetailsUrl, fixturesFilename)
			fmt.Println("Downloading fixture file: ", fullPathofJsonFile)

		}

		jsonFile, err := os.Open(fullPathofJsonFile)
		if err != nil {
			fmt.Println(err)
		}

		defer jsonFile.Close()

		// read our opened jsonFile as a byte array.
		byteValue, _ := ioutil.ReadAll(jsonFile)
		json.Unmarshal(byteValue, &gameweekDetails)

		for i, _ := range gameweekDetails {

			if gameweekDetails[i].Finished != true {
				Completedgw = gameweekDetails[i].Event - 1
				Currentgw = gameweekDetails[i].Event

				CurrentGWStarted = gameweekDetails[i].Started

				break OuterLoop

			}
		}

	}
	fmt.Println("------------------------------------------------------------------")

	return Completedgw, Currentgw, CurrentGWStarted

}

var CurrentGWInProgress bool = false

func GWInProgress() bool {
	// get the current latest gameweek
	_, Currentgw, _ := GetLatestGW()
	// Currentgw := 2
	targetGameweek := Currentgw
	gwstring := strconv.Itoa(targetGameweek)

	// Download or refresh data about a particular gameweek
	fixtureDetailsUrl := "https://fantasy.premierleague.com/api/fixtures?event=" + gwstring
	fixturesFilename := "fixturedetailsgw_" + gwstring + ".json"
	fullPathofJsonFile := DownloadJsonFile(fixtureDetailsUrl, fixturesFilename)

	// Probes the downloaded json file
	jsonFile, err := os.Open(fullPathofJsonFile)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	// read our opened jsonFile as a byte array.
	byteValue, _ := ioutil.ReadAll(jsonFile)
	json.Unmarshal(byteValue, &gameweekDetails)

	// declare counters
	totalNumberOfFixtures := 0
	totalStartedFixtures := 0
	totalNotStartedFixtures := 0
	totalFinishedFixtures := 0

	for i, _ := range gameweekDetails {

		totalNumberOfFixtures++
		// Add count of fixture that has been started
		if gameweekDetails[i].Started {
			totalStartedFixtures++
		}

		if !gameweekDetails[i].Started {
			// Add count of fixture that has not been started
			totalNotStartedFixtures++
		}

		if gameweekDetails[i].Finished {
			totalFinishedFixtures++
		}

	}

	// Gameweek in progress is false if the total number of fixtures and total number of finished fixtures is the same
	if totalNumberOfFixtures == totalFinishedFixtures {
		CurrentGWInProgress = false
	} else if totalStartedFixtures >= 1 && totalFinishedFixtures != totalNumberOfFixtures {
		CurrentGWInProgress = true
	}

	// fmt.Println("Current gameweek", Currentgw, "Gameweek total fixture:", totalNumberOfFixtures)
	// fmt.Println("Total of started fixtures :", totalStartedFixtures)
	// fmt.Println("Total of finished fixtures :", totalFinishedFixtures)

	return CurrentGWInProgress
}

// DownloadJson File downloads the corresponding json file to /tmp depending on the url
func DownloadJsonFile(getApiUrl string, nameOfTheFile string) string {

	// Declare the variable that will hold th argument value
	JasonFullFilePath := "/tmp/" + nameOfTheFile

	_, err := exec.Command("/usr/bin/wget", getApiUrl, "-O", JasonFullFilePath).Output()

	if err != nil {
		fmt.Printf("%s", err)
	}

	// output := string(out[:])
	// fmt.Println(output)

	return JasonFullFilePath

}

// Manager is a struct equivalent of the json file consumed

type PlayerAllocation struct {
	Picks []struct {
		Element       int  `json:"element"`
		Position      int  `json:"position"`
		IsCaptain     bool `json:"is_captain"`
		IsViceCaptain bool `json:"is_vice_captain"`
		Multiplier    int  `json:"multiplier"`
	} `json:"picks"`
	EntryHistory struct {
	} `json:"entry_history"`
	Subs []interface{} `json:"subs"`
}

func InsertPlayerAllocation() {

	// Detect up to what gameweek playerallocation and playerstats should be populated on
	InsertUpto := InsertUpGW()

	// Create a database object
	// connStr := "user=goapiserviceuser dbname=goapi password=oracle host=192.168.56.101 sslmode=disable"
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"

	// Initialise the db con cobject
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)
	}
	defer db.Close()

	managerIDs := GetManagerIDs()

	for _, id := range managerIDs {

		// Convert integer to string
		e := strconv.Itoa(id)

		for gw := 1; gw <= InsertUpto; gw++ {
			fmt.Println("Inserting player allocation on gameweek:", gw, "for managerid: ", e)
			gameweekno := strconv.Itoa(gw)

			// Iterate over list of manager id to gather data
			managerURL := "https://draft.premierleague.com/api/entry/" + e + "/event/" + gameweekno

			response, err := http.Get(managerURL)
			// fmt.Println("API call return code when called: ", response.StatusCode)

			// Checks if there is an error or the status code is not 200 on the API call
			if err != nil || response.StatusCode != 200 {
				fmt.Println("---------------------------------------------------------")
				fmt.Println("API call", managerURL)
				fmt.Println("manager id ", e, " API call returned with ", response.StatusCode)
				fmt.Printf("The HTTP request failed with error %s\n", err)

				fmt.Println("---------------------------------------------------------")
			} else {
				body, _ := ioutil.ReadAll(response.Body)

				// Creates a new object of playerallocation
				var playerallocation PlayerAllocation

				err := json.Unmarshal([]byte(body), &playerallocation)
				if err != nil {
					panic(err)
				}

				// Interare over all gameweek data per player and insert them to a table
				for i, _ := range playerallocation.Picks {
					// fmt.Println("Player ID : ", playerallocation.Picks[i].Element)
					// fmt.Println("Position : ", playerallocation.Picks[i].Position)

					// Assign the values that you will be storing in the table into a variable
					managerid := id
					gameweeknumber := gameweekno
					playerid := playerallocation.Picks[i].Element
					playerposition := playerallocation.Picks[i].Position

					// fmt.Println("Manager ID : ", managerid)
					// fmt.Println("Game Week Number : ", gameweeknumber)
					// fmt.Println("Player ID : ", playerid)
					// fmt.Println("Position : ", playerposition)

					// Create the syntax for the insert statement

					insertOnPlayerAllocation := `
				INSERT INTO rpl.playerallocation (playerid
				,managerid
				,gameweeknumber
				,playerposition)
				VALUES ($1, $2, $3, $4)`

					// Insert the data into the database

					_, err = db.Exec(insertOnPlayerAllocation,
						playerid,
						managerid,
						gameweeknumber,
						playerposition,
					)

					if err != nil {
						panic(err)
					}

					commitStatement := "commit"
					_, err = db.Exec(commitStatement)

					if err != nil {
						panic(err)
					}

				}

			}
		}

	}

}

type PlayerStats struct {
	Elements []struct {
		ID    int `json:"id"`
		Stats struct {
			Minutes         int    `json:"minutes"`
			GoalsScored     int    `json:"goals_scored"`
			Assists         int    `json:"assists"`
			CleanSheets     int    `json:"clean_sheets"`
			GoalsConceded   int    `json:"goals_conceded"`
			OwnGoals        int    `json:"own_goals"`
			PenaltiesSaved  int    `json:"penalties_saved"`
			PenaltiesMissed int    `json:"penalties_missed"`
			YellowCards     int    `json:"yellow_cards"`
			RedCards        int    `json:"red_cards"`
			Saves           int    `json:"saves"`
			Bonus           int    `json:"bonus"`
			Bps             int    `json:"bps"`
			Influence       string `json:"influence"`
			Creativity      string `json:"creativity"`
			Threat          string `json:"threat"`
			IctIndex        string `json:"ict_index"`
			TotalPoints     int    `json:"total_points"`
			InDreamteam     bool   `json:"in_dreamteam"`
		} `json:"stats"`
		Explain []struct {
			Fixture int `json:"fixture"`
			Stats   []struct {
				Identifier string `json:"identifier"`
				Points     int    `json:"points"`
				Value      int    `json:"value"`
			} `json:"stats"`
		} `json:"explain"`
	} `json:"elements"`
}

func InsertPlayerStats(myFileName string, gameweek string) {

	// Create a database object
	// connStr := "user=goapiserviceuser dbname=goapi password=oracle host=192.168.56.101 sslmode=disable"
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"

	// Initialise the db con cobject
	db, err := sql.Open("postgres", connStr)

	// Check for error and panic is there is one
	if err != nil {
		panic(err)

	}
	defer db.Close()

	// Open the json file
	jsonFile, err := os.Open(myFileName)
	if err != nil {
		fmt.Println(err)
	}

	// Close json buffer before closing
	defer jsonFile.Close()

	// read our opened jsonFile as a byte array.
	byteValue, _ := ioutil.ReadAll(jsonFile)

	// we initialize our Users array
	var playerstats PlayerStats

	json.Unmarshal(byteValue, &playerstats)

	for i, _ := range playerstats.Elements {
		// fmt.Println("-------------------Players Stats-------------------")
		// fmt.Println("Player ID : ", playerstats.Elements[i].ID)
		// fmt.Println("Minutes Played : ", playerstats.Elements[i].Stats.Minutes)
		// fmt.Println("Total Points : ", playerstats.Elements[i].Stats.TotalPoints)

		// Assign the values that you will be storing in the table into a variable
		id := playerstats.Elements[i].ID
		Gameweek := gameweek
		Minutes := playerstats.Elements[i].Stats.Minutes
		GoalsScored := playerstats.Elements[i].Stats.GoalsScored
		Assists := playerstats.Elements[i].Stats.Assists
		CleanSheets := playerstats.Elements[i].Stats.CleanSheets
		GoalsConceded := playerstats.Elements[i].Stats.GoalsConceded
		OwnGoals := playerstats.Elements[i].Stats.OwnGoals
		PenaltiesSaved := playerstats.Elements[i].Stats.PenaltiesSaved
		PenaltiesMissed := playerstats.Elements[i].Stats.PenaltiesMissed
		YellowCards := playerstats.Elements[i].Stats.YellowCards
		RedCards := playerstats.Elements[i].Stats.RedCards
		Saves := playerstats.Elements[i].Stats.Saves
		Bonus := playerstats.Elements[i].Stats.Bonus
		Bps := playerstats.Elements[i].Stats.Bps
		Influence := playerstats.Elements[i].Stats.Influence
		Creativity := playerstats.Elements[i].Stats.Creativity
		Threat := playerstats.Elements[i].Stats.Threat
		IctIndex := playerstats.Elements[i].Stats.IctIndex
		TotalPoints := playerstats.Elements[i].Stats.TotalPoints

		// Create the syntax for the insert statement

		InsertOnPlayerStats := `
		INSERT INTO rpl.playerstats (id
		,Gameweek       
		,Minutes        
		,GoalsScored    
		,Assists        
		,CleanSheets    
		,GoalsConceded  
		,OwnGoals       
		,PenaltiesSaved 
		,PenaltiesMissed
		,YellowCards    
		,RedCards       
		,Saves          
		,Bonus          
		,Bps            
		,Influence      
		,Creativity     
		,Threat         
		,IctIndex       
		,TotalPoints   
		) VALUES ($1,$2,$3,$4,$5,$6,
		$7,$8,$9,$10,$11,$12,$13,$14,
		$15,$16,$17,$18,$19,$20)`

		// Insert the data into the datab
		_, err = db.Exec(InsertOnPlayerStats,
			id,
			Gameweek,
			Minutes,
			GoalsScored,
			Assists,
			CleanSheets,
			GoalsConceded,
			OwnGoals,
			PenaltiesSaved,
			PenaltiesMissed,
			YellowCards,
			RedCards,
			Saves,
			Bonus,
			Bps,
			Influence,
			Creativity,
			Threat,
			IctIndex,
			TotalPoints,
		)

		if err != nil {
			panic(err)
		}

	}

}

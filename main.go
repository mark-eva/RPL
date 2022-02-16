package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"net/http"
	m "rpl/src/manager"
	"strconv"
	"strings"
	"text/template"

	"github.com/jmoiron/sqlx"

	_ "github.com/lib/pq"
)

// Defines the endpoint that will be invoked depending on the link that the user uses
func handleRequests() {
	http.HandleFunc("/", homePage)
	http.HandleFunc("/latestgameweek", returnCombinedGameweek)
	http.HandleFunc("/index", leagueTableHML)
	http.HandleFunc("/form", managerFormHandler)
	log.Fatal(http.ListenAndServe(":80", nil))
}

func managerFormHandler(w http.ResponseWriter, r *http.Request) {
	// tmpl, _ := template.ParseFiles("layout.html")
	var tmpl = template.Must(template.ParseFiles("manager_form.html"))
	// tmpl.Execute(w, user_current)

	err := tmpl.ExecuteTemplate(w, "ManagerForm", "")
	if err != nil {
		fmt.Println(err)
	}
}

// Declare the function that will handle the http request
func homePage(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome to the HomePage!")
	fmt.Println("Endpoint Hit: homePage")
}

// Servers the returnAllArticles
func returnCombinedGameweek(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Endpoint Hit: returnAllArticles")

	// Extract latest gameweek data
	data := m.ReturnCombinedGameweek()

	buffer := &bytes.Buffer{} // creates IO Writer
	writer := csv.NewWriter(buffer)

	for _, value := range data {
		writer.Write(value)
		// err := writer.Write(value)
		// checkError("Cannot write to buffer", err)
	}

	writer.Flush()

	w.Header().Set("Content-Type", "text/csv") // setting the content type header to text/csv
	w.Header().Set("Content-Disposition", "attachment;filename=latestgameweekcombined.csv")
	w.Write(buffer.Bytes())

}

// func dbSelect() {

// }

// Define a struct
type LeagueTableInfo struct {
	POS      string
	L        string
	TEAMNAME string
	MANAGER  string
	GW       string
	PTS      string
	DIF      string
	MP       string
	GS       string
	A        string
	CS       string
	BESTGW   string
	WORSTGW  string
	AVGGW    string
}

func leagueTableHML(w http.ResponseWriter, r *http.Request) {

	// Parses all manager id from  the form page
	r.ParseForm()
	managers, present := r.Form["m"] //m=[12133, 12656, 75765]
	if !present || len(managers) == 0 {
		fmt.Println("Managers not present")
	}

	fmt.Println(strings.Join(managers, ","))

	// Declare a slice that will be used as a query parameter to display the table
	var leagueList []int
	var newValidManagerIDs []int

	// Check each element on the list if they exist in the database
	for _, values := range managers {

		fmt.Println("Verifying the following manager id: ", values)

		// convert the string value into an int
		managerIDconverted, err := strconv.Atoi(values)
		if err != nil {
			panic(err)
		}

		managerExist := m.ManagerIdRegistered(managerIDconverted)
		// fmt.Println("Manager exist in the system : ", managerIDconverted, " ", managerExist)

		// Decision logic regarding existence of manager id in the system
		if !managerExist {
			// Check the validity of the the manager id
			fmt.Println("---------------------------------------------------------------------------------------")
			fmt.Println("Managerid:", values, "does not exist in DB. Checking the validity of the manager id....")

			// Check the validity of the manager id
			managerValid, leagueID := m.ManagerValid(managerIDconverted)

			if !managerValid {
				fmt.Println("Managerid validity is:", managerValid, "Default league ID therefore is:", leagueID)

			} else {
				fmt.Println("Managerid validity is:", managerValid, "League ID is ", leagueID)
				// Insert data regarding league id into rpl.league_manager_allocation and rpl.league
				mynewmid := m.InsertLeagueInfo(leagueID)
				fmt.Println("new managerids: ", mynewmid)

				// Loop through each new manager id and append to the slice
				for _, mymanagerid := range mynewmid {
					newValidManagerIDs = append(newValidManagerIDs, mymanagerid)

				}

				// As we've added a new Manager ID, we need to refresh
				// m.RefreshData()

				// Add League ID to an int slice that will be used as a query parameter on the main query
				leagueList = append(leagueList, leagueID)

			}

			fmt.Println("---------------------------------------------------------------------------------------")

		} else {
			// Get the corresponding league id for the manager id  if already exist in the system
			// Check the validity of the manager id
			_, leagueIDInDB := m.ManagerValid(managerIDconverted)

			fmt.Println("---------------------------------------------------------------------------------------")
			fmt.Println("Managerid:", values, "exist in the database. Retrieving the corresponding league ID")
			fmt.Println("League ID is: ", leagueIDInDB)
			fmt.Println("---------------------------------------------------------------------------------------")

			// Add League ID to an int slice that will be used as a query parameter on the main query

			leagueList = append(leagueList, leagueIDInDB)

		}

	}

	// Insert new manager ids into
	m.InsertLatestInfo(newValidManagerIDs)

	// Find out if new managers has been added into the database

	totalNewManagers := len(newValidManagerIDs)

	//Refresh playerallocation
	if totalNewManagers >= 1 {
		m.TruncateTable("playerallocation")
		m.InsertPlayerAllocation()
	}

	fmt.Println("List of league ids to be used in a query")
	fmt.Println(leagueList)
	fmt.Println("---------------------------------------------------------------------------------------")
	fmt.Println("Number of new managers:", totalNewManagers)
	fmt.Println("---------------------------------------------------------------------------------------")

	// Instantiate db connection
	connStr := "user=goapiserviceuser dbname=goapi password=oracle host=localhost sslmode=disable"
	db, err := sqlx.Connect("postgres", connStr)

	if err != nil {
		panic(err)
	}

	defer db.Close()

	// Build the query

	query, args, err := sqlx.In(`select base."POS."
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
		  , a.leagueset
		from rpl.manager a, rpl.managerhistory b
		where a.id = b.managerid
		and b.gameweek = (select max(gameweek) from rpl.managerhistory)
-- 		and a.leagueset = 17739
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
	and bestgw.managerid = base.id
	and base.leagueset in (?)`, leagueList)

	query = db.Rebind(query)
	rows, err := db.Query(query, args...)

	if err != nil {
		log.Println(err)
	}

	defer rows.Close()
	user_current := []LeagueTableInfo{}

	for rows.Next() {
		p := LeagueTableInfo{}
		err := rows.Scan(&p.POS, &p.L, &p.TEAMNAME, &p.MANAGER, &p.GW, &p.PTS,
			&p.DIF, &p.MP, &p.GS, &p.A, &p.CS, &p.BESTGW, &p.WORSTGW, &p.AVGGW)

		if err != nil {
			fmt.Println(err)
			continue
		}

		user_current = append(user_current, p)
	}
	fmt.Println(user_current)

	// tmpl, _ := template.ParseFiles("layout.html")
	var tmpl = template.Must(template.ParseFiles("layout.html"))
	// tmpl.Execute(w, user_current)

	err = tmpl.ExecuteTemplate(w, "Index", user_current)
	if err != nil {
		fmt.Println(err)
	}

}

func main() {
	fmt.Println("Starting the application...")

	// Invoke a concurrent data refresh
	go m.DataRefresh()

	// Handle all incoming request
	handleRequests()

}

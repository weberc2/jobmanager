package main

import (
	"log"
	"net/http"
	"os"

	"github.com/weberc2/httpeasy"
	. "github.com/weberc2/jobmanager/jobmanager"
)

func main() {
	db, err := OpenDatabase()
	if err != nil {
		JSONLog("db_open_error", err.Error())
		log.Fatal(err)
	}
	defer db.Close()

	JSONLog("db_open_succeeded", "")

	if err := http.ListenAndServe(":8084", httpeasy.Register(
		httpeasy.JSONLog(os.Stderr),
		httpeasy.Route{
			Path:   "/jobs/",
			Method: "GET",
			Handler: func(r httpeasy.Request) httpeasy.Response {
				jobs, err := jobs(db)
				if err != nil {
					return httpeasy.InternalServerError(err.Error())
				}
				return httpeasy.Ok(httpeasy.JSON(jobs))
			},
		},
		httpeasy.Route{
			Path:   "/jobs/{job_id}/runs/",
			Method: "GET",
			Handler: func(r httpeasy.Request) httpeasy.Response {
				runs, err := runsOfJob(db, JobID(r.Vars["job_id"]))
				if err != nil {
					return httpeasy.InternalServerError(err.Error())
				}
				return httpeasy.Ok(httpeasy.JSON(runs))
			},
		},
	)); err != nil {
		JSONLog("http_server_error", err.Error())
		log.Fatal(err)
	}
}

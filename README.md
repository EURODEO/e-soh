# E-SOH
## EURODEO
The RODEO project develops a user interface and Application Programming Interfaces (API) for accessing meteorological datasets declared as High Value Datasets (HVD) by the EU Implementing Regulation (EU) 2023/138 under the EU Open Data Directive (EU) 2019/1024. The project also fosters the engagement between data providers and data users for enhancing the understanding of technical solutions being available for sharing and accessing the HVD datasets.
This project provides a sustainable and standardized system for sharing real-time surface weather observations in line with the HVD regulation and WMO WIS 2.0 strategy. The real-time surface weather observations are made available through open web services, so that they can be accessed by anyone.
## Near real-time observational data
E-SOH is part of the RODEO project. The goal for this project is to make near real-time weather observations from land based station easily available. The data will be published on both a message queue using MQTT and [EDR](https://ogcapi.ogc.org/edr/) compliant APIs. Metadata will also be made available through [OGC Records](https://ogcapi.ogc.org/records/) APIs. The system architecture is portable, scalable and modular system for taking into account possible extension to additional networks and datasets (e.g. 3rd party surface observations) in the future.
## Structure
The E-SOH service consist of three parts.
  * Ingestion API
	- The ingest API handles incoming observation from partners. It will verify the inncomming data and publish them to the MQTT queue and submit the observations to the datastore.
  * Datastore
    - Datastore stores all observations for 24h and is the backend storage for EDR and Records APIs.
  * EDR API
    - The EDR api is the main endpoint to download observational data available in E-SOH.
   * Records API
      - The records api will serve all metadata for each timeseries available in E-SOH.

## Usage
All three parts of the service is kept in this repository and can be built and setup using docker-compose. For more documentation on each part of the service see README.md in each relevant folder in this repository.
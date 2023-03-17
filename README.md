# Assignment 3
## Introduction
This assignment requires you to build a Streamlit interface using Snowflake sqlalchemy in Python for the 5 queries assigned to your team (TPC-DS). The goal is to implement the Qualification Substitution Parameters as variables and validate inputs using allowed values from the metadata.

## Requirements
To complete this assignment, you will need the following:
- Python 3.6 or higher
- Snowflake sqlalchemy
- Streamlit
- Access to the TPC-DS queries assigned to your team

## Getting Started
- Clone the repository and navigate to the project directory.
- Install the required dependencies using pip: pip install -r requirements.txt
- Open the main.py file and update the Snowflake connection settings with your Snowflake account information.
- Run the application using Streamlit: streamlit run main.py
- Once the application is running, enter the required inputs for the TPC-DS queries assigned to your team.

## Implementation Details
- The Snowflake sqlalchemy library is used to connect to the Snowflake database and execute the queries.
- The Streamlit library is used to build the user interface for the application.
- The Qualification Substitution Parameters are implemented as variables to allow for dynamic filtering of the queries.
- Metadata validation is implemented to ensure that only allowed values are entered in the inputs. This includes validating date fields to ensure that they only contain valid values.

## References
- Snowflake sqlalchemy
- Streamlit
- TPC-DS

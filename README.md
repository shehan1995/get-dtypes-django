# Process DTypes

Welcome to my Django project!. This README will guide you through setting up and running the project locally.

DEMO : https://react-frontend-aa41f.web.app/

_**Please note** that, for DEMO you can upload only files upto **30MB** due to a limitation in hosted server_

## Project Overview

Our Django project aims to provide DTypes of  uploaded .csv/excel dataset.
This will expose endpoint to get DTypes.
## Prerequisites

Before you begin, ensure you have met the following requirements:

- Python 
- Django 
- (Optional) Virtual environment (e.g., virtualenv or pipenv)

## Installation

To install the project dependencies, follow these steps:

1. Clone the repository to your local machine:

    ```bash
    git clone https://github.com/shehan1995/get-dtypes-django
    ```

2. Navigate to the project directory:

    ```bash
    cd get-dtypes-django
    ```

3. Install the required Python packages:

    ```bash
    pip install -r requirements.txt
    ```

## Configuration

Before running the project, make sure to configure any necessary settings. Typically, you'll need to set up your database configurations in `settings.py`, along with any other project-specific settings.

## Running the Project

To run the project locally, follow these steps:

1. Navigate to the project directory if you're not already there:

    ```bash
    cd get-dtypes-django
    ```

2. Run the Django development server:

    ```bash
    python manage.py runserver
    ```

3. Open your web browser and visit `http://localhost:8000` to view the project.

Sample CURL

```bash
curl --location 'http://127.0.0.1:8000/server/process/' \
--form 'file=@"{file path}"'
```

## Contact

If you have any questions or concerns, feel free to contact me at asrajakaruna@gmail.com.

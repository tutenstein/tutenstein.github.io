import functions_framework
import requests
from datetime import datetime
import os
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import json
from collections import defaultdict
from git import Repo  # GitPython kütüphanesi için


def scheduled_scraper():
  try:
      # Read existing JSON file if it exists
      existing_data = []
      if os.path.exists('sponsor_list/Worker_and_Temporary_Worker.json'):
          with open('sponsor_list/Worker_and_Temporary_Worker.json', 'r', encoding='utf-8') as f:
              existing_data = pd.read_json(f).to_dict('records')

      # Your scraping logic here
      page_url = "https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers"
      response = requests.get(page_url)
      response.raise_for_status()   # Adjust based on your scraping logic
      soup = BeautifulSoup(response.content, 'html.parser')
      link_tag = soup.find('a', class_='govuk-link gem-c-attachment__link', href=True)
      if link_tag:
        csv_url = link_tag['href']
        if not csv_url.startswith('http'):
            csv_url = 'https://www.gov.uk' + csv_url  # Ensure the URL is absolute

        csv_response = requests.get(csv_url)
        csv_response.raise_for_status()
        csv_content = StringIO(csv_response.text)
        df = pd.read_csv(csv_content)
        output_path = 'sponsor_list/Worker_and_Temporary_Worker.json'
        df.to_json(output_path, orient='records', force_ascii=False)
        print(f"Scraped data saved to {output_path}")

      # Convert new data to records
      new_data = df.to_dict('records')

      # Track changes
      changes = {
          'added': [],
          'removed': [],
          'timestamp': datetime.now().isoformat()
      }

      # Create dictionaries for easier comparison
      existing_dict = {record.get('Organisation Name', ''): record for record in existing_data}
      new_dict = {record.get('Organisation Name', ''): record for record in new_data}

      current_time = datetime.now().strftime("%Y-%m-%d")
      
      # Find added and modified records
      for org_name, new_record in new_dict.items():
          if org_name not in existing_dict:
              new_record['insertion_time'] = current_time
              changes['added'].append(new_record)


      # Find removed records
      for org_name in existing_dict:
          if org_name not in new_dict:
              removed_record = existing_dict[org_name].copy()
              removed_record['deletion_time'] = current_time
              changes['removed'].append(removed_record)

      # After calculating changes
      print(f"Found {len(changes['added'])} new records")
      print(f"Found {len(changes['removed'])} removed records")
      

      # Load existing changes if the file exists
      changes_path = 'sponsor_list/changes.json'
      if os.path.exists(changes_path):
          with open(changes_path, 'r', encoding='utf-8') as f:
              existing_changes = json.load(f)
      else:
          existing_changes = defaultdict(list)

      # Update changes
      existing_changes['added'].extend(changes['added'])
      existing_changes['removed'].extend(changes['removed'])
      existing_changes['timestamp'] = datetime.now().isoformat()

      # Save updated changes to the JSON file
      with open(changes_path, 'w', encoding='utf-8') as f:
          json.dump(existing_changes, f, indent=2, ensure_ascii=False)
      print(f"Updated changes saved to {changes_path}")

      # Save the new data as before
      df.to_json('sponsor_list/Worker_and_Temporary_Worker.json', orient='records', force_ascii=False)
      print(f"Updated data saved to sponsor_list/Worker_and_Temporary_Worker.json")

      # GitHub'a push işlemi
      repo = Repo('.')  # Mevcut dizindeki repo
      repo.index.add(['sponsor_list/Worker_and_Temporary_Worker.json', 'sponsor_list/changes.json'])
      repo.index.commit(f"Update sponsor data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
      origin = repo.remote(name='origin')
      origin.push()

      print("Files successfully pushed to GitHub")
      
      return {
          'success': True, 
          'timestamp': datetime.now().isoformat(), 
          'changes': existing_changes
      }
  
  except Exception as e:
      print(f"Error occurred: {str(e)}")
      return {'success': False, 'error': str(e)}

if __name__ == '__main__':
    scheduled_scraper() 
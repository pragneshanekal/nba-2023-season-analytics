import aiohttp
import asyncio
import boto3
from nba_api.stats.static import teams
from nba_api.stats.endpoints import teaminfocommon
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import requests

# Function to upload DataFrame to S3
def upload_to_s3(df, bucket_name, file_name):
    s3_client = boto3.client('s3')
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())
    print(f"{file_name} has been uploaded to S3 bucket {bucket_name}")

# Retry configuration for API calls
@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10), retry=retry_if_exception_type(requests.exceptions.RequestException))
async def fetch_team_info_common(session, team_id):
    url = f"https://stats.nba.com/stats/teaminfocommon?LeagueID=00&TeamID={team_id}"
    async with session.get(url, timeout=60) as response:
        response.raise_for_status()
        data = await response.json()
        return pd.DataFrame(data['resultSets'][0]['rowSet'], columns=data['resultSets'][0]['headers']).iloc[0]

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10), retry=retry_if_exception_type(requests.exceptions.RequestException))
async def fetch_game_ids(session, game_date):
    url = f"https://stats.nba.com/stats/scoreboardV2?GameDate={game_date}&LeagueID=00"
    async with session.get(url, timeout=60) as response:
        response.raise_for_status()
        data = await response.json()
        return [game['GAME_ID'] for game in data['resultSets'][0]['rowSet']]

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10), retry=retry_if_exception_type(requests.exceptions.RequestException))
async def fetch_box_score(session, game_id):
    url = f"https://stats.nba.com/stats/boxscoretraditionalv2?GameID={game_id}"
    async with session.get(url, timeout=60) as response:
        response.raise_for_status()
        data = await response.json()
        team_stats_df = pd.DataFrame(data['resultSets'][1]['rowSet'], columns=data['resultSets'][1]['headers'])
        player_stats_df = pd.DataFrame(data['resultSets'][0]['rowSet'], columns=data['resultSets'][0]['headers'])
        return team_stats_df, player_stats_df

async def fetch_and_upload_team_details(bucket_name):
    nba_teams = teams.get_teams()
    teams_df = pd.DataFrame(nba_teams)
    teams_df['conference'] = None
    teams_df['division'] = None

    async with aiohttp.ClientSession() as session:
        tasks = []
        for index, row in teams_df.iterrows():
            team_id = row['id']
            tasks.append(fetch_team_info_common(session, team_id))

        team_info_list = await asyncio.gather(*tasks)

        for index, team_info in enumerate(team_info_list):
            try:
                teams_df.at[index, 'conference'] = team_info['TEAM_CONFERENCE']
                teams_df.at[index, 'division'] = team_info['TEAM_DIVISION']
            except Exception as e:
                print(f"An error occurred for team ID {nba_teams[index]['id']}: {e}")

    teams_file_name = 'team_details/nba_teams_conference_division.csv'
    upload_to_s3(teams_df, bucket_name, teams_file_name)

async def fetch_and_upload_stats(game_ids, bucket_name, date_folder, game_date):
    all_team_stats = []
    all_player_stats = []

    async with aiohttp.ClientSession() as session:
        tasks = []
        for game_id in game_ids:
            tasks.append(fetch_box_score(session, game_id))

        results = await asyncio.gather(*tasks)

        for game_id, (team_stats_df, player_stats_df) in zip(game_ids, results):
            try:
                team_stats_df['GAME_ID'] = game_id
                player_stats_df['GAME_ID'] = game_id
                all_team_stats.append(team_stats_df)
                all_player_stats.append(player_stats_df)
            except Exception as e:
                print(f"An error occurred for game ID {game_id}: {e}")

    if all_team_stats:
        all_team_stats_df = pd.concat(all_team_stats, ignore_index=True)
        team_stats_file_name = f'{date_folder}/nba_team_stats_{game_date}.csv'
        upload_to_s3(all_team_stats_df, bucket_name, team_stats_file_name)
    else:
        print(f"No team stats to concatenate for date {game_date}")

    if all_player_stats:
        all_player_stats_df = pd.concat(all_player_stats, ignore_index=True)
        player_stats_file_name = f'{date_folder}/nba_player_stats_{game_date}.csv'
        upload_to_s3(all_player_stats_df, bucket_name, player_stats_file_name)
    else:
        print(f"No player stats to concatenate for date {game_date}")

async def main():
    bucket_name = 'mynbadatabucket'
    await fetch_and_upload_team_details(bucket_name)

    start_date = datetime(2023, 10, 24)
    end_date = datetime(2024, 4, 14)
    date = start_date

    while date <= end_date:
        game_date = date.strftime('%Y-%m-%d')
        month_folder = date.strftime('%Y-%m')
        date_folder = f'game_stats/{month_folder}'

        async with aiohttp.ClientSession() as session:
            game_ids = await fetch_game_ids(session, game_date)
            await fetch_and_upload_stats(game_ids, bucket_name, date_folder, game_date)

        date += timedelta(days=1)

if __name__ == "__main__":
    asyncio.run(main())

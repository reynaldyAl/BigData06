import requests
import base64
import pandas as pd
import time
from datetime import datetime
import os
from dotenv import load_dotenv

class SpotifyAPI:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self.get_token()
        self.base_url = "https://api.spotify.com/v1/"
    
    def get_token(self):
        """Mendapatkan token akses dari Spotify API"""
        auth_url = 'https://accounts.spotify.com/api/token'
        
        # Encode client_id dan client_secret dalam format base64
        auth_header = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        
        headers = {
            'Authorization': f'Basic {auth_header}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        
        data = {'grant_type': 'client_credentials'}
        
        response = requests.post(auth_url, headers=headers, data=data)
        
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data['access_token']
            print("Token berhasil didapatkan!")
            return access_token
        else:
            print(f"Error mendapatkan token: {response.status_code}")
            print(response.json())
            return None
    
    def make_api_request(self, endpoint, params=None):
        """Membuat request ke Spotify API"""
        headers = {
            'Authorization': f'Bearer {self.token}'
        }
        
        url = self.base_url + endpoint
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:  # Token expired
            print("Token expired. Mendapatkan token baru...")
            self.token = self.get_token()
            return self.make_api_request(endpoint, params)
        else:
            print(f"Error pada API request: {response.status_code}")
            print(response.json())
            return None
    
    def get_top_artists(self, limit=50):
        """Mendapatkan artis-artis populer"""
        # Karena tidak ada endpoint langsung untuk top artists,
        # kita bisa menggunakan search dengan popularity sorting
        results = self.make_api_request('search', {
            'q': 'year:2023-2024',  # Filter untuk konten terbaru
            'type': 'artist',
            'limit': limit
        })
        
        if results and 'artists' in results:
            artists = results['artists']['items']
            artists_data = []
            
            for artist in artists:
                artist_info = {
                    'id': artist['id'],
                    'name': artist['name'],
                    'popularity': artist['popularity'],
                    'followers': artist['followers']['total'],
                    'genres': ', '.join(artist['genres']) if artist['genres'] else '',
                    'image_url': artist['images'][0]['url'] if artist['images'] else ''
                }
                artists_data.append(artist_info)
            
            return pd.DataFrame(artists_data)
        return pd.DataFrame()
    
    def get_artist_top_tracks(self, artist_id):
        """Mendapatkan track populer dari artist tertentu"""
        results = self.make_api_request(f'artists/{artist_id}/top-tracks', {'market': 'ID'})
        
        if results and 'tracks' in results:
            tracks = results['tracks']
            tracks_data = []
            
            for track in tracks:
                track_info = {
                    'id': track['id'],
                    'name': track['name'],
                    'popularity': track['popularity'],
                    'album_name': track['album']['name'],
                    'release_date': track['album']['release_date'],
                    'duration_ms': track['duration_ms'],
                    'explicit': track['explicit']
                }
                tracks_data.append(track_info)
            
            return pd.DataFrame(tracks_data)
        return pd.DataFrame()
    
    def get_audio_features(self, track_ids):
        """Mendapatkan audio features untuk track tertentu"""
        if isinstance(track_ids, str):
            track_ids = [track_ids]
        
        # API hanya bisa handle 100 track IDs dalam sekali request
        results = []
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i:i+100]
            batch_result = self.make_api_request('audio-features', {'ids': ','.join(batch)})
            if batch_result and 'audio_features' in batch_result:
                results.extend(batch_result['audio_features'])
            time.sleep(0.5)  # Menghindari rate limit
        
        if results:
            return pd.DataFrame(results)
        return pd.DataFrame()
    
    def get_new_releases(self, limit=50, country='ID'):
        """Mendapatkan album baru yang dirilis"""
        results = self.make_api_request('browse/new-releases', {
            'limit': limit,
            'country': country
        })
        
        if results and 'albums' in results:
            albums = results['albums']['items']
            albums_data = []
            
            for album in albums:
                artists = ', '.join([artist['name'] for artist in album['artists']])
                album_info = {
                    'id': album['id'],
                    'name': album['name'],
                    'artists': artists,
                    'release_date': album['release_date'],
                    'total_tracks': album['total_tracks'],
                    'album_type': album['album_type'],
                    'image_url': album['images'][0]['url'] if album['images'] else ''
                }
                albums_data.append(album_info)
            
            return pd.DataFrame(albums_data)
        return pd.DataFrame()
    
    def save_to_csv(self, dataframe, filename):
        """Menyimpan DataFrame ke CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"{filename}_{timestamp}.csv"
        dataframe.to_csv(filepath, index=False)
        print(f"Data berhasil disimpan ke {filepath}")
        return filepath

def main():
    # Masukkan client ID dan client secret dari Spotify Developer Dashboard Anda
    client_id = "YOUR_CLIENT_ID"
    client_secret = "YOUR_CLIENT_SECRET"
    
    spotify = SpotifyAPI(client_id, client_secret)
    
    # 1. Mendapatkan artis populer
    top_artists = spotify.get_top_artists(limit=50)
    if not top_artists.empty:
        spotify.save_to_csv(top_artists, "spotify_top_artists")
    
    # 2. Mengambil data track populer dan audio features
    all_tracks = pd.DataFrame()
    all_audio_features = pd.DataFrame()
    
    if not top_artists.empty:
        for artist_id in top_artists['id'][:10]:  # Mengambil 10 artis populer
            artist_tracks = spotify.get_artist_top_tracks(artist_id)
            if not artist_tracks.empty:
                all_tracks = pd.concat([all_tracks, artist_tracks])
                
                # Mendapatkan audio features untuk track
                track_ids = artist_tracks['id'].tolist()
                audio_features = spotify.get_audio_features(track_ids)
                if not audio_features.empty:
                    all_audio_features = pd.concat([all_audio_features, audio_features])
            
            time.sleep(1)  # Menghindari rate limit
    
    if not all_tracks.empty:
        spotify.save_to_csv(all_tracks, "spotify_top_tracks")
    
    if not all_audio_features.empty:
        spotify.save_to_csv(all_audio_features, "spotify_audio_features")
    
    # 3. Mendapatkan new releases
    new_releases = spotify.get_new_releases(limit=50)
    if not new_releases.empty:
        spotify.save_to_csv(new_releases, "spotify_new_releases")

if __name__ == "__main__":
    main()
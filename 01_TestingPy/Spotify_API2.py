import requests
import base64
import pandas as pd
import time
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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
            print(f"URL: {url}")
            print(f"Params: {params}")
            if hasattr(response, 'json'):
                try:
                    print(response.json())
                except:
                    print(f"Response text: {response.text}")
            return None
    
    def get_top_artists(self, limit=50):
        """Mendapatkan artis-artis populer"""
        # Gunakan pendekatan yang lebih andal: mencari genre populer
        genres = ['pop', 'rock', 'hip hop', 'k-pop', 'edm']
        
        all_artists = []
        for genre in genres:
            print(f"Mencari artis genre {genre}...")
            results = self.make_api_request('search', {
                'q': f'genre:{genre}',
                'type': 'artist',
                'limit': limit // len(genres)  # Bagi limit sesuai jumlah genre
            })
            
            if results and 'artists' in results and 'items' in results['artists']:
                all_artists.extend(results['artists']['items'])
                print(f"Menemukan {len(results['artists']['items'])} artis untuk genre {genre}")
            
            time.sleep(0.5)  # Hindari rate limiting
        
        # Alternatif kedua jika pencarian genre tidak memberikan hasil
        if not all_artists:
            print("Mencoba mendapatkan artis dari playlist featured...")
            # Mendapatkan artis dari playlist featured
            featured = self.make_api_request('browse/featured-playlists', {
                'limit': 5,
                'country': 'ID'
            })
            
            if featured and 'playlists' in featured and 'items' in featured['playlists']:
                playlists = featured['playlists']['items']
                
                for playlist in playlists:
                    playlist_id = playlist['id']
                    tracks = self.make_api_request(f'playlists/{playlist_id}/tracks', {'limit': 10})
                    
                    if tracks and 'items' in tracks:
                        for item in tracks['items']:
                            if item and 'track' in item and item['track'] and 'artists' in item['track']:
                                for artist in item['track']['artists']:
                                    # Dapatkan detail artis
                                    artist_detail = self.make_api_request(f"artists/{artist['id']}")
                                    if artist_detail:
                                        all_artists.append(artist_detail)
                                    time.sleep(0.2)
        
        print(f"Total artis yang dikumpulkan: {len(all_artists)}")
        
        # Proses data artis
        artists_data = []
        for artist in all_artists:
            if not artist:
                continue
                
            artist_info = {
                'id': artist['id'],
                'name': artist['name'],
                'popularity': artist['popularity'],
                'followers': artist['followers']['total'],
                'genres': ', '.join(artist['genres']) if artist['genres'] else '',
                'image_url': artist['images'][0]['url'] if artist['images'] and len(artist['images']) > 0 else ''
            }
            artists_data.append(artist_info)
        
        # Hapus duplikasi berdasarkan ID artis
        df = pd.DataFrame(artists_data)
        if not df.empty:
            df = df.drop_duplicates(subset='id')
            print(f"Jumlah artis unik: {len(df)}")
            
        return df
    
    def get_artist_top_tracks(self, artist_id):
        """Mendapatkan track populer dari artist tertentu"""
        print(f"Mendapatkan top tracks untuk artis ID: {artist_id}")
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
                    'explicit': track['explicit'],
                    'artist_id': artist_id
                }
                tracks_data.append(track_info)
            
            print(f"Mendapatkan {len(tracks_data)} tracks untuk artis ID: {artist_id}")
            return pd.DataFrame(tracks_data)
            
        print(f"Tidak ada tracks yang ditemukan untuk artis ID: {artist_id}")
        return pd.DataFrame()
    
    def get_audio_features(self, track_ids):
        """Mendapatkan audio features untuk track tertentu"""
        if isinstance(track_ids, str):
            track_ids = [track_ids]
        
        if not track_ids:
            print("Tidak ada track IDs untuk diproses")
            return pd.DataFrame()
            
        print(f"Mendapatkan audio features untuk {len(track_ids)} tracks")
        
        # API hanya bisa handle 100 track IDs dalam sekali request
        results = []
        for i in range(0, len(track_ids), 100):
            batch = track_ids[i:i+100]
            print(f"Memproses batch {i//100 + 1} dengan {len(batch)} tracks")
            batch_result = self.make_api_request('audio-features', {'ids': ','.join(batch)})
            
            if batch_result and 'audio_features' in batch_result:
                # Filter None values
                valid_features = [f for f in batch_result['audio_features'] if f]
                results.extend(valid_features)
                print(f"Menambahkan {len(valid_features)} audio features")
            else:
                print("Tidak ada audio features yang dikembalikan untuk batch ini")
                
            time.sleep(0.5)  # Menghindari rate limit
        
        if results:
            return pd.DataFrame(results)
        return pd.DataFrame()
    
    def get_new_releases(self, limit=50, country='ID'):
        """Mendapatkan album baru yang dirilis"""
        print(f"Mendapatkan {limit} new releases untuk negara {country}")
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
            
            print(f"Menemukan {len(albums_data)} new releases")
            return pd.DataFrame(albums_data)
            
        print("Tidak menemukan new releases")
        return pd.DataFrame()
    
    def save_to_csv(self, dataframe, filename):
        """Menyimpan DataFrame ke CSV"""
        if dataframe.empty:
            print(f"Tidak ada data untuk disimpan ke {filename}")
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = f"{filename}_{timestamp}.csv"
        dataframe.to_csv(filepath, index=False)
        print(f"Data berhasil disimpan ke {filepath}")
        return filepath
    
    # Metode tambahan: Mendapatkan rekomendasi track
    def get_recommendations(self, seed_tracks=None, seed_artists=None, limit=50):
        """Mendapatkan rekomendasi track berdasarkan seed tracks atau artists"""
        params = {'limit': limit}
        
        if seed_tracks:
            if isinstance(seed_tracks, list):
                seed_tracks = seed_tracks[:5]  # Max 5 seeds
                params['seed_tracks'] = ','.join(seed_tracks)
            else:
                params['seed_tracks'] = seed_tracks
                
        if seed_artists:
            if isinstance(seed_artists, list):
                seed_artists = seed_artists[:5]  # Max 5 seeds
                params['seed_artists'] = ','.join(seed_artists)
            else:
                params['seed_artists'] = seed_artists
        
        print(f"Mendapatkan rekomendasi dengan parameter: {params}")
        results = self.make_api_request('recommendations', params)
        
        if results and 'tracks' in results:
            tracks = results['tracks']
            tracks_data = []
            
            for track in tracks:
                artists = ', '.join([artist['name'] for artist in track['artists']])
                track_info = {
                    'id': track['id'],
                    'name': track['name'],
                    'artists': artists,
                    'popularity': track['popularity'],
                    'album_name': track['album']['name'],
                    'release_date': track['album']['release_date'],
                    'explicit': track['explicit']
                }
                tracks_data.append(track_info)
            
            print(f"Mendapatkan {len(tracks_data)} rekomendasi track")
            return pd.DataFrame(tracks_data)
            
        print("Tidak mendapatkan rekomendasi track")
        return pd.DataFrame()

def main():
    # Mengambil kredensial dari environment variables
    client_id = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
    
    # Memeriksa apakah kredensial tersedia
    if not client_id or not client_secret:
        print("Error: SPOTIFY_CLIENT_ID dan SPOTIFY_CLIENT_SECRET tidak ditemukan di file .env")
        print("Pastikan file .env berisi:")
        print("SPOTIFY_CLIENT_ID=your_client_id_here")
        print("SPOTIFY_CLIENT_SECRET=your_client_secret_here")
        return
    
    spotify = SpotifyAPI(client_id, client_secret)
    
    # 1. Mendapatkan artis populer
    print("\n--- MENGAMBIL DATA ARTIS POPULER ---")
    top_artists = spotify.get_top_artists(limit=50)
    if not top_artists.empty:
        spotify.save_to_csv(top_artists, "spotify_top_artists")
    else:
        print("Tidak berhasil mendapatkan artis populer.")
    
    # 2. Mengambil data track populer dan audio features
    print("\n--- MENGAMBIL DATA TRACK POPULER ---")
    all_tracks = pd.DataFrame()
    all_audio_features = pd.DataFrame()
    
    # Jika artis ditemukan, ambil track mereka
    if not top_artists.empty:
        # Ambil sample artis (maksimal 10)
        artist_sample = top_artists['id'].tolist()
        if len(artist_sample) > 10:
            artist_sample = artist_sample[:10]
            
        print(f"Mengambil tracks dari {len(artist_sample)} artis populer")
        
        for artist_id in artist_sample:
            artist_tracks = spotify.get_artist_top_tracks(artist_id)
            if not artist_tracks.empty:
                all_tracks = pd.concat([all_tracks, artist_tracks])
                
                # Mendapatkan audio features untuk track
                track_ids = artist_tracks['id'].tolist()
                audio_features = spotify.get_audio_features(track_ids)
                if not audio_features.empty:
                    all_audio_features = pd.concat([all_audio_features, audio_features])
            
            time.sleep(1)  # Menghindari rate limit
    # Fallback jika tidak mendapatkan artis
    else:
        print("Mencoba fallback: mendapatkan tracks dari new releases")
        new_releases = spotify.get_new_releases(limit=10)
        if not new_releases.empty:
            # Dapatkan detail album untuk mendapatkan tracks
            for album_id in new_releases['id'].tolist()[:5]:  # Ambil 5 album saja
                album_tracks = spotify.make_api_request(f'albums/{album_id}/tracks', {'limit': 10})
                if album_tracks and 'items' in album_tracks:
                    track_ids = [track['id'] for track in album_tracks['items']]
                    # Dapatkan detail tracks
                    for i in range(0, len(track_ids), 20):
                        batch = track_ids[i:i+20]
                        tracks_info = spotify.make_api_request('tracks', {'ids': ','.join(batch)})
                        if tracks_info and 'tracks' in tracks_info:
                            for track in tracks_info['tracks']:
                                if track:
                                    track_data = {
                                        'id': track['id'],
                                        'name': track['name'],
                                        'popularity': track['popularity'],
                                        'album_name': track['album']['name'],
                                        'release_date': track['album']['release_date'],
                                        'duration_ms': track['duration_ms'],
                                        'explicit': track['explicit']
                                    }
                                    all_tracks = pd.concat([all_tracks, pd.DataFrame([track_data])])
                                    
                    # Dapatkan audio features
                    audio_features = spotify.get_audio_features(track_ids)
                    if not audio_features.empty:
                        all_audio_features = pd.concat([all_audio_features, audio_features])
    
    # Simpan data tracks dan audio features
    if not all_tracks.empty:
        spotify.save_to_csv(all_tracks, "spotify_top_tracks")
    else:
        print("Tidak berhasil mendapatkan data tracks.")
    
    if not all_audio_features.empty:
        spotify.save_to_csv(all_audio_features, "spotify_audio_features")
    else:
        print("Tidak berhasil mendapatkan audio features.")
    
    # 3. Mendapatkan new releases
    print("\n--- MENGAMBIL DATA NEW RELEASES ---")
    new_releases = spotify.get_new_releases(limit=50)
    if not new_releases.empty:
        spotify.save_to_csv(new_releases, "spotify_new_releases")
    else:
        print("Tidak berhasil mendapatkan new releases.")
        
    # 4. Mendapatkan rekomendasi track (tambahan)
    print("\n--- MENGAMBIL DATA REKOMENDASI TRACK ---")
    # Gunakan track ID dari all_tracks jika ada
    if not all_tracks.empty:
        seed_tracks = all_tracks['id'].tolist()[:5]  # Ambil 5 track sebagai seed
        recommendations = spotify.get_recommendations(seed_tracks=seed_tracks)
        if not recommendations.empty:
            spotify.save_to_csv(recommendations, "spotify_recommendations")

if __name__ == "__main__":
    main()
import requests
import base64
import pandas as pd
import time
from datetime import datetime
import os
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

class SpotifyAPI:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = self.get_token()
        self.base_url = "https://api.spotify.com/v1/"
        # Tambahkan informasi market untuk memastikan data tersedia
        self.default_market = "ID"
    
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
        
        # Tambahkan market ke semua requests untuk memastikan availability
        if params is None:
            params = {}
        if 'market' not in params and endpoint != 'audio-features':
            params['market'] = self.default_market
            
        # Tambahkan jeda sebelum request untuk mengurangi risiko rate limiting
        time.sleep(0.5)
        
        try:
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
                if hasattr(response, 'text'):
                    print(f"Response: {response.text[:200]}...")  # Print sebagian response
                return None
        except Exception as e:
            print(f"Exception saat request API: {str(e)}")
            return None
    
    def get_top_artists(self, limit=50):
        """Mendapatkan artis-artis populer"""
        # Gunakan pendekatan yang lebih andal: mencari beberapa genre populer
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
        results = self.make_api_request(f'artists/{artist_id}/top-tracks', {'market': self.default_market})
        
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
                    'artist_id': artist_id,
                    'artist_name': track['artists'][0]['name'] if track['artists'] else 'Unknown'
                }
                tracks_data.append(track_info)
            
            print(f"Mendapatkan {len(tracks_data)} tracks untuk artis ID: {artist_id}")
            return pd.DataFrame(tracks_data)
            
        print(f"Tidak ada tracks yang ditemukan untuk artis ID: {artist_id}")
        return pd.DataFrame()
    
    def get_track_basic_features(self, track_ids):
        """
        Mendapatkan informasi dasar tentang track sebagai alternatif untuk audio features
        yang mengalami error 403 Forbidden
        """
        if isinstance(track_ids, str):
            track_ids = [track_ids]
        
        if not track_ids:
            print("Tidak ada track IDs untuk diproses")
            return pd.DataFrame()
            
        print(f"Mendapatkan informasi dasar untuk {len(track_ids)} tracks")
        
        all_track_data = []
        # API bisa handle max 50 track IDs dalam sekali request
        for i in range(0, len(track_ids), 50):
            batch = track_ids[i:i+50]
            print(f"Memproses batch {i//50 + 1} dengan {len(batch)} tracks")
            
            # Dapatkan info track dasar
            tracks_info = self.make_api_request('tracks', {'ids': ','.join(batch)})
            
            if tracks_info and 'tracks' in tracks_info:
                for track in tracks_info['tracks']:
                    if track:
                        artists = ", ".join([artist['name'] for artist in track['artists']])
                        # Ekstrak informasi yang tersedia dalam endpoint tracks
                        track_data = {
                            'id': track['id'],
                            'name': track['name'],
                            'popularity': track['popularity'],
                            'duration_ms': track['duration_ms'],
                            'explicit': track['explicit'],
                            'artists': artists,
                            'album_name': track['album']['name'],
                            'release_date': track['album']['release_date'],
                            'track_number': track['track_number'],
                            'disc_number': track['disc_number']
                        }
                        all_track_data.append(track_data)
                print(f"Menambahkan {len(tracks_info['tracks'])} track info")
            else:
                print("Tidak mendapatkan info track")
                
            time.sleep(0.5)  # Hindari rate limiting
        
        if all_track_data:
            return pd.DataFrame(all_track_data)
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
    
    def get_album_tracks(self, album_id):
        """Mengambil daftar track dari album"""
        print(f"Mengambil tracks dari album ID: {album_id}")
        results = self.make_api_request(f'albums/{album_id}/tracks', {
            'limit': 50,
            'market': self.default_market
        })
        
        if results and 'items' in results:
            tracks = results['items']
            tracks_data = []
            
            for track in tracks:
                artists = ', '.join([artist['name'] for artist in track['artists']])
                track_info = {
                    'id': track['id'],
                    'name': track['name'],
                    'disc_number': track['disc_number'],
                    'track_number': track['track_number'],
                    'duration_ms': track['duration_ms'],
                    'artists': artists,
                    'album_id': album_id
                }
                tracks_data.append(track_info)
            
            print(f"Menemukan {len(tracks_data)} tracks di album")
            return pd.DataFrame(tracks_data)
            
        print(f"Tidak menemukan tracks untuk album ID: {album_id}")
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
    
    def get_categories(self, limit=50):
        """Mendapatkan kategori musik di Spotify"""
        print(f"Mendapatkan {limit} kategori musik")
        results = self.make_api_request('browse/categories', {
            'limit': limit,
            'country': self.default_market,
            'locale': 'id_ID'  # Sesuaikan dengan bahasa/locale yang sesuai
        })
        
        if results and 'categories' in results and 'items' in results['categories']:
            categories = results['categories']['items']
            categories_data = []
            
            for category in categories:
                category_info = {
                    'id': category['id'],
                    'name': category['name'],
                    'icon_url': category['icons'][0]['url'] if category['icons'] else ''
                }
                categories_data.append(category_info)
            
            print(f"Menemukan {len(categories_data)} kategori musik")
            return pd.DataFrame(categories_data)
            
        print("Tidak menemukan kategori musik")
        return pd.DataFrame()

def main():
    # Mengambil kredensial dari environment variables
    client_id = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
    
    # Memeriksa apakah kredensial tersedia
    if not client_id or not client_secret:
        print("Error: SPOTIFY_CLIENT_ID dan SPOTIFY_CLIENT_SECRET tidak ditemukan di file .env")
        print("Pastikan file .env berisi:")
        print("SPOTIFY_CLIENT_ID= in env_file")
        print("SPOTIFY_CLIENT_SECRET= in env_file")
        return
    
    spotify = SpotifyAPI(client_id, client_secret)
    
    # 1. Mendapatkan artis populer
    print("\n--- MENGAMBIL DATA ARTIS POPULER ---")
    top_artists = spotify.get_top_artists(limit=200)
    if not top_artists.empty:
        spotify.save_to_csv(top_artists, "spotify_top_artists")
    else:
        print("Tidak berhasil mendapatkan artis populer.")
    
    # 2. Mengambil data track populer 
    print("\n--- MENGAMBIL DATA TRACK POPULER ---")
    all_tracks = pd.DataFrame()
    
    # Jika artis ditemukan, ambil track mereka
    if not top_artists.empty:
        # Ambil sample artis (maksimal 20 untuk mendapatkan lebih banyak data)
        artist_sample = top_artists['id'].tolist()
        if len(artist_sample) > 20:
            artist_sample = artist_sample[:20]
            
        print(f"Mengambil tracks dari {len(artist_sample)} artis populer")
        
        for artist_id in artist_sample:
            artist_tracks = spotify.get_artist_top_tracks(artist_id)
            if not artist_tracks.empty:
                all_tracks = pd.concat([all_tracks, artist_tracks])
                
        if not all_tracks.empty:
            spotify.save_to_csv(all_tracks, "spotify_top_tracks")
        else:
            print("Tidak mendapatkan track populer.")
    
    # 3. Mendapatkan NEW RELEASES (yang berhasil sebelumnya)
    print("\n--- MENGAMBIL DATA NEW RELEASES ---")
    new_releases = spotify.get_new_releases(limit=50) # note : new release cuma bisa sampai 50
    if not new_releases.empty:
        spotify.save_to_csv(new_releases, "spotify_new_releases")
    else:
        print("Tidak berhasil mendapatkan new releases.")
    
    # 5. Mendapatkan Categories (alternative dataset)
    print("\n--- MENGAMBIL DATA KATEGORI MUSIK ---")
    categories = spotify.get_categories(limit=50)
    if not categories.empty:
        spotify.save_to_csv(categories, "spotify_categories")
    
    # 6. Mendapatkan Track Details dari album baru
    # Ambil album baru dan extract track-nya (max 5 album)
    print("\n--- MENGAMBIL DATA TRACK DARI ALBUM BARU ---")
    all_album_tracks = pd.DataFrame()
    
    if not new_releases.empty:
        album_sample = new_releases['id'].tolist()[:200]  # Ambil 100 album baru
        for album_id in album_sample:
            album_tracks = spotify.get_album_tracks(album_id)
            if not album_tracks.empty:
                # Tambahkan info album
                album_info = new_releases[new_releases['id'] == album_id]
                if not album_info.empty:
                    album_name = album_info['name'].iloc[0]
                    album_artists = album_info['artists'].iloc[0]
                    album_tracks['album_name'] = album_name
                    album_tracks['album_artists'] = album_artists
                    
                all_album_tracks = pd.concat([all_album_tracks, album_tracks])
    
    if not all_album_tracks.empty:
        spotify.save_to_csv(all_album_tracks, "spotify_album_tracks")

        # 8. Mendapatkan informasi detail dari track terpopuler
    print("\n--- MENGAMBIL DETAIL TRACK POPULER ---")
    track_details = pd.DataFrame()
    
    if not all_tracks.empty:
        # Pilih track paling populer, hingga 100 track
        popular_tracks = all_tracks.sort_values('popularity', ascending=False)
        track_ids = popular_tracks['id'].tolist()
        if len(track_ids) > 100:
            track_ids = track_ids[:100]
        
        # Dapatkan detail track
        track_details = spotify.get_track_basic_features(track_ids)
        
        if not track_details.empty:
            spotify.save_to_csv(track_details, "spotify_track_details")
        else:
            print("Tidak berhasil mendapatkan detail track.")

        # Tampilkan rekapitulasi dataset
    datasets = {
        "Artis Populer": top_artists,
        "Track Populer": all_tracks,
        "Kategori Musik": categories,
        "Tracks dari Album": all_album_tracks,
        "Detail Track": track_details,
        "New Releases": new_releases
    }
    
    print("\n=== REKAPITULASI DATASET ===")
    for name, df in datasets.items():
        if not df.empty:
            print(f"{name}: {len(df)} baris")
        else:
            print(f"{name}: Tidak berhasil dikumpulkan")


if __name__ == "__main__":  
    main()
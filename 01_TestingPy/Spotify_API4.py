
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
    
    def get_playlist_details(self, playlist_id, market=None):
        """Mendapatkan detail dari playlist spesifik berdasarkan ID"""
        print(f"Mengambil detail untuk playlist: {playlist_id}")
        
        if market is None:
            market = self.default_market
        
        params = {
            'market': market,
            'fields': 'id,name,description,followers,images,owner,tracks.total,external_urls,public,collaborative'
        }
        
        results = self.make_api_request(f'playlists/{playlist_id}', params)
        
        if results:
            # Ekstrak data penting dari playlist
            playlist_info = {
                'id': results['id'],
                'name': results['name'],
                'description': results['description'],
                'owner_id': results['owner']['id'],
                'owner_name': results['owner']['display_name'],
                'followers': results['followers']['total'] if 'followers' in results else 0,
                'tracks_total': results['tracks']['total'],
                'image_url': results['images'][0]['url'] if results['images'] else '',
                'spotify_url': results.get('external_urls', {}).get('spotify', ''),
                'public': results.get('public', None),
                'collaborative': results.get('collaborative', False)
            }
            print(f"Detail playlist berhasil didapatkan: {playlist_info['name']}")
            return pd.DataFrame([playlist_info])
        
        print(f"Gagal mendapatkan detail playlist: {playlist_id}")
        return pd.DataFrame()
    
    def get_multiple_playlists(self, playlist_ids=None, limit=50):
        """Mendapatkan detail dari beberapa playlist populer"""
        print(f"Mengambil detail untuk {limit} playlists")
        
        # Jika tidak ada playlist IDs, gunakan beberapa playlist populer sebagai default
        if not playlist_ids:
            # Playlist populer dari berbagai genre/kategori
            default_playlists = [
                '37i9dQZF1DXcBWIGoYBM5M',  # Today's Top Hits
                '37i9dQZEVXbMDoHDwVN2tF',  # Global Top 50
                '37i9dQZF1DX0XUsuxWHRQd',  # RapCaviar
                '37i9dQZF1DWXRqgorJj26U',  # Rock Classics
                '37i9dQZF1DX4dyzvuaRJ0n',  # Lo-Fi Beats
                '37i9dQZF1DX4sWSpwq3LiO',  # Peaceful Piano
                '37i9dQZF1DX10zKzsJ2jva',  # Viva Latino
                '37i9dQZF1DWY4xHQp97fN6',  # Get Turnt
                '37i9dQZF1DX4WYpdgoIcn6',  # Chill Hits
                '37i9dQZF1DX8tZsk68tuDw',  # Dance Rising
                '37i9dQZF1DX6GwdWRQMQpq',  # Rock This
                '37i9dQZF1DX4o1oenSJRJd',  # All Out 2000s
                '37i9dQZF1DX1lVhptIYRda',  # Hot Country
                '37i9dQZF1DX76Wlfdnj7AP',  # Beast Mode
                '37i9dQZF1DXbTxeAdrVG2l',  # K-Pop Daebak
            ]
            playlist_ids = default_playlists[:limit]
        
        all_playlists = pd.DataFrame()
        
        for playlist_id in playlist_ids:
            playlist_data = self.get_playlist_details(playlist_id)
            if not playlist_data.empty:
                all_playlists = pd.concat([all_playlists, playlist_data])
            time.sleep(0.5)  # Jeda untuk menghindari rate limiting
        
        print(f"Berhasil mengambil {len(all_playlists)} playlists")
        return all_playlists
    
    def get_playlist_tracks(self, playlist_id, limit=100):
        """Mendapatkan tracks dari playlist tertentu"""
        print(f"Mendapatkan tracks dari playlist ID: {playlist_id}")
        
        # Dapatkan informasi playlist dulu
        playlist_info = self.make_api_request(f'playlists/{playlist_id}', {
            'fields': 'name,owner.display_name'
        })
        
        playlist_name = playlist_info['name'] if playlist_info and 'name' in playlist_info else 'Unknown'
        owner_name = playlist_info['owner']['display_name'] if playlist_info and 'owner' in playlist_info else 'Unknown'
        
        # Gunakan pagination untuk mendapatkan semua track
        tracks = []
        offset = 0
        total_fetched = 0
        max_per_request = 50  # API limit
        
        while total_fetched < limit:
            params = {
                'limit': min(max_per_request, limit - total_fetched),
                'offset': offset,
                'market': self.default_market,
                'fields': 'items(track(id,name,artists,album,duration_ms,explicit,popularity,preview_url,external_urls),added_at)'
            }
            
            results = self.make_api_request(f'playlists/{playlist_id}/tracks', params)
            
            if not results or 'items' not in results or not results['items']:
                break
                
            tracks.extend(results['items'])
            
            new_items = len(results['items'])
            total_fetched += new_items
            
            # Stop jika kita mendapatkan kurang dari yang diminta (akhir playlist)
            if new_items < params['limit']:
                break
                
            offset += max_per_request
            time.sleep(0.5)  # Avoid rate limiting
            
        # Proses track data
        tracks_data = []
        
        for item in tracks:
            if 'track' not in item or not item['track']:
                continue
                
            track = item['track']
            # Skip local tracks atau yang tidak ada metadata
            if track.get('id') is None:
                continue
                
            artists = ', '.join([artist['name'] for artist in track['artists']])
            album_name = track['album']['name'] if 'album' in track else 'Unknown'
            
            track_info = {
                'id': track['id'],
                'name': track['name'],
                'artists': artists,
                'album_name': album_name,
                'duration_ms': track['duration_ms'],
                'duration_min': round(track['duration_ms'] / 60000, 2),
                'explicit': track.get('explicit', False),
                'popularity': track.get('popularity', 0),
                'added_at': item.get('added_at', ''),
                'playlist_id': playlist_id,
                'playlist_name': playlist_name,
                'owner_name': owner_name,
                'preview_url': track.get('preview_url', ''),
                'spotify_url': track.get('external_urls', {}).get('spotify', '')
            }
            tracks_data.append(track_info)
        
        print(f"Menemukan {len(tracks_data)} tracks dalam playlist {playlist_name}")
        return pd.DataFrame(tracks_data)
    
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
        print("SPOTIFY_CLIENT_ID=your_client_id_here")
        print("SPOTIFY_CLIENT_SECRET=your_client_secret_here")
        return
    
    spotify = SpotifyAPI(client_id, client_secret)
    
    # 1. Mendapatkan artis populer
    print("\n--- MENGAMBIL DATA ARTIS POPULER ---")
    top_artists = spotify.get_top_artists(limit=100)  # Tingkatkan limit menjadi 100
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
    
    # 3. Mendapatkan NEW RELEASES
    print("\n--- MENGAMBIL DATA NEW RELEASES ---")
    countries = ['ID', 'US', 'GB']  # Indonesia, US, UK
    all_new_releases = pd.DataFrame()
    
    for country in countries:
        country_releases = spotify.get_new_releases(limit=50, country=country)
        if not country_releases.empty:
            country_releases['source_country'] = country
            all_new_releases = pd.concat([all_new_releases, country_releases])
    
    if not all_new_releases.empty:
        # Hapus duplikat album berdasarkan ID
        all_new_releases = all_new_releases.drop_duplicates(subset='id')
        spotify.save_to_csv(all_new_releases, "spotify_new_releases")
    else:
        print("Tidak berhasil mendapatkan new releases.")
    
    # 4. Mendapatkan Playlist Populer (pengganti featured playlists)
    print("\n--- MENGAMBIL DATA PLAYLISTS POPULER ---")
    popular_playlists = spotify.get_multiple_playlists(limit=15)  # Ambil 15 playlist populer
    if not popular_playlists.empty:
        spotify.save_to_csv(popular_playlists, "spotify_popular_playlists")
    else:
        print("Tidak berhasil mendapatkan playlists populer.")

    # 5. Mendapatkan Track dari Playlists Populer
    print("\n--- MENGAMBIL TRACK DARI PLAYLISTS POPULER ---")
    all_playlist_tracks = pd.DataFrame()

    if not popular_playlists.empty:
        # Ambil 3 playlist dengan followers terbanyak
        top_playlists = popular_playlists.sort_values('followers', ascending=False).head(3)
        
        for _, playlist in top_playlists.iterrows():
            playlist_id = playlist['id']
            playlist_name = playlist['name']
            print(f"Mengambil tracks dari playlist: {playlist_name}")
            
            # Ambil 100 track dari setiap playlist (bisa disesuaikan)
            playlist_tracks = spotify.get_playlist_tracks(playlist_id, limit=100)
            if not playlist_tracks.empty:
                all_playlist_tracks = pd.concat([all_playlist_tracks, playlist_tracks])

    if not all_playlist_tracks.empty:
        spotify.save_to_csv(all_playlist_tracks, "spotify_playlist_tracks")
    else:
        print("Tidak berhasil mendapatkan tracks dari playlists.")
    
    # 6. Mendapatkan kategori musik
    print("\n--- MENGAMBIL DATA KATEGORI MUSIK ---")
    categories = spotify.get_categories(limit=50)
    if not categories.empty:
        spotify.save_to_csv(categories, "spotify_categories")
    else:
        print("Tidak berhasil mendapatkan kategori musik.")
    
    # 7. Mendapatkan track dari album baru
    print("\n--- MENGAMBIL DATA TRACK DARI ALBUM BARU ---")
    all_album_tracks = pd.DataFrame()
    
    if not all_new_releases.empty:
        # Ambil 10 album pertama dari new releases untuk data lebih banyak
        album_sample = all_new_releases['id'].tolist()
        if len(album_sample) > 10:
            album_sample = album_sample[:10]
            
        print(f"Mengambil tracks dari {len(album_sample)} album")
        
        for album_id in album_sample:
            album_tracks = spotify.get_album_tracks(album_id)
            if not album_tracks.empty:
                all_album_tracks = pd.concat([all_album_tracks, album_tracks])
                
        if not all_album_tracks.empty:
            spotify.save_to_csv(all_album_tracks, "spotify_album_tracks")
        else:
            print("Tidak mendapatkan track dari album.")
    
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
    
    print("\n=== PENGUMPULAN DATA SELESAI ===")
    print("Semua data berhasil dikumpulkan dan disimpan dalam format CSV.")
    
    # Tampilkan rekapitulasi dataset
    datasets = {
        "Artis Populer": top_artists,
        "Track Populer": all_tracks,
        "New Releases": all_new_releases,
        "Playlists Populer": popular_playlists,
        "Tracks dari Playlists": all_playlist_tracks,
        "Kategori Musik": categories,
        "Tracks dari Album": all_album_tracks,
        "Detail Track": track_details
    }
    
    print("\n=== REKAPITULASI DATASET ===")
    for name, df in datasets.items():
        if not df.empty:
            print(f"{name}: {len(df)} baris")
        else:
            print(f"{name}: Tidak berhasil dikumpulkan")

if __name__ == "__main__":
    main()
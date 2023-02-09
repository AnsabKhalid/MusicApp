package MusicCrud;

import Model.Artist;
import Model.ArtistSongs;
import Model.DataSource;

import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {

        DataSource dataSource = new DataSource();

        if (dataSource.open()) {
            System.out.println("DataBase Source Open Successfully");
        }

        /*List<Artist> artists = new ArrayList<>();
        artists = dataSource.artistList();*/

        List<Artist> artists = dataSource.artistList(DataSource.NO_ORDER);

        if (artists == null) {
            System.out.println("Your database is empty");
            return;
        } else {
            if(artists.size() == 0) {
                System.out.println("No artist found");
            } else {
                System.out.println("=========ARTISTS DETAIL=========");
                for (Artist artist:artists) {
                    System.out.println("ID = " + artist.getId() + " -- " + "Name = " +artist.getName());
                }
            }
        }

        List<String> albums = dataSource.artistsAlbums("Ariana Grande", DataSource.DESC_ORDER);

        if (albums == null)  {
            System.out.println("No Albums of Artist By that Name");
            return;
        } else {
            if (albums.size() == 0) {
                System.out.println("No Album of Artist Found By that Name");
            } else {
                for (String albumName:albums) {
                    System.out.println(albumName);
                }
            }
        }

        List<ArtistSongs> artistSongs = dataSource.artistSongsSearch("7 rings", DataSource.ASC_ORDER);
        if (artistSongs == null) {
            System.out.println("No Artists for specified Song");
        } else {
            if (artistSongs.size() == 0) {
                System.out.println("artist for specified song is not found");
            } else {
                for (ArtistSongs artistSong:artistSongs) {
                    System.out.println("Artist Name = " + artistSong.getArtistName());
                    System.out.println("Album Name = " + artistSong.getAlbumName());
                    System.out.println("Track No = " + artistSong.getTrack());
                }
            }
        }

        dataSource.setQuerySongMetaData();
        int count = dataSource.getCount(DataSource.TABLE_SONGS);
        System.out.println("You Have " + count + " Recorded Songs in Database");

        if (dataSource.createViewForSongArtists()) {
            System.out.println("View is Created for Artists Songs");
        }

        System.out.println("Please Insert The Song Title");
        Scanner scanner = new Scanner(System.in);
        String songTitle = scanner.nextLine();

        List<ArtistSongs> artistSongsList = dataSource.querySongTitleInfoView(songTitle);
        if (artistSongsList.isEmpty()) {
            System.out.println("Empty, Could not find this Song");
            return;
        }

        System.out.println("======Data From View======");
        for (ArtistSongs artistSong: artistSongsList) {
            System.out.println("Artist_Name: " + artistSong.getArtistName());
            System.out.println("Album_Name: " + artistSong.getAlbumName());
            System.out.println("Track_No: " + artistSong.getTrack());
        }

        dataSource.close();
    }
}
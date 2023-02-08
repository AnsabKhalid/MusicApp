package Model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DataSource {
    private Connection connection;
    private PreparedStatement querySongTitleInfoViewStatement;
    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;
    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    public static final String DB_NAME = "sound.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:C:\\sqliteProject\\Music\\" + DB_NAME;
    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID = "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";

    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME = 2;
    public static final int INDEX_ALBUM_ARTIST = 3;
    public static final String TABLE_ARTISTS = "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";

    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME = 2;
    public static final String TABLE_SONGS = "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";

    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK = 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;

    public static final int NO_ORDER = 0;
    public static final int ASC_ORDER = 1;
    public static final int DESC_ORDER = 2;

    public static final String QUERY_ARTIST_ALBUMS_LIST = "SELECT * FROM " + TABLE_ARTISTS;
    public static final String QUERY_ARTIST_ALBUMS_LIST_SORT = " ORDER BY " + TABLE_ARTISTS + "." +
            COLUMN_ARTIST_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ARTIST_LIST =
            "SELECT " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUMS +
                    " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." +
                    COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
                    " WHERE " + TABLE_ARTISTS + "." + COLUMN_ALBUM_NAME + " = \"";
    public static final String QUERY_ARTIST_LIST_SORT = " ORDER BY " + TABLE_ALBUMS + "." +
            COLUMN_ARTIST_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ARTIST_SONGS = "SELECT " + TABLE_ARTISTS + "." +
            COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK + " FROM " + TABLE_SONGS + " INNER JOIN " +
            TABLE_ALBUMS + " ON " + TABLE_SONGS + "." + COLUMN_SONG_ALBUM + " = " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_ID + " INNER JOIN " + TABLE_ARTISTS +
            " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS +
            "." + COLUMN_ARTIST_ID + " WHERE " + TABLE_SONGS + "." + COLUMN_SONG_TITLE +
            "= \"";

    public static final String QUERY_ARTIST_SONGS_SORT = " ORDER BY " + TABLE_ARTISTS +
            "." + COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME +
            " COLLATE NOCASE ";

    public static final String TABLE_ARTISTS_SONG_VIEW = "artists_list";

    public static final String CREATE_ARTISTS_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
            TABLE_ARTISTS_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + '.' + COLUMN_ARTIST_NAME +
            ", " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " +
            TABLE_SONGS + '.' + COLUMN_SONG_TRACK + ", " + TABLE_SONGS + '.' + COLUMN_SONG_TITLE
            + " FROM " + TABLE_SONGS + " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS +
            '.' + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_ID + " INNER " +
            "JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_ARTIST + " = "
            + TABLE_ARTISTS + '.' + COLUMN_ARTIST_ID + " ORDER BY " + TABLE_ARTISTS + '.' +
            COLUMN_ARTIST_NAME + ", " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME + ", " +
            TABLE_SONGS + '.' + COLUMN_SONG_TRACK;

    public static final String QUERY_SONG_TITLE_INFO_VIEW = "SELECT " + COLUMN_ARTIST_NAME +
            ", " + COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTISTS_SONG_VIEW
            + " WHERE " + COLUMN_SONG_TITLE + " = \"";

    public static final String QUERY_SONG_TITLE_INFO_SQL_STMNT_VIEW = "SELECT " + COLUMN_ARTIST_NAME +
            ", " + COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTISTS_SONG_VIEW
            + " WHERE " + COLUMN_SONG_TITLE + " = ?";

    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS + '(' +
            COLUMN_ARTIST_NAME + ')' + "VALUES(?)";
    public static final String INSERT_ALBUMS = "INSERT INTO " + TABLE_ALBUMS + '(' +
            COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ')' + "VALUES(?, ?)";
    public static final String INSERT_SONGS = "INSERT INTO " + TABLE_SONGS + '(' +
            COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + COLUMN_SONG_ALBUM + ')' +
            "VALUES(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " +
            TABLE_ARTISTS + " WHERE " + COLUMN_ALBUM_NAME + " = ?";
    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ARTIST_ID + " FROM " +
            TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_NAME + " = ?";

    public boolean open() {
        try {
            connection = DriverManager.getConnection(CONNECTION_STRING);
            querySongTitleInfoViewStatement = connection.prepareStatement(
                    QUERY_SONG_TITLE_INFO_SQL_STMNT_VIEW
            );
            insertIntoArtists = connection.prepareStatement(INSERT_ARTIST,
                    Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = connection.prepareStatement(INSERT_ALBUMS,
                    Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = connection.prepareStatement(INSERT_SONGS);
            queryArtist = connection.prepareStatement(QUERY_ARTIST);
            queryAlbum = connection.prepareStatement(QUERY_ALBUM);

            return true;
        } catch (SQLException e) {
            System.out.printf("Something went Wrong " + e.getMessage());
            return false;
        }
    }
    public void close() {
        try {
            if (querySongTitleInfoViewStatement != null) {
                querySongTitleInfoViewStatement.close();
            }
            if (insertIntoArtists != null) {
                insertIntoArtists.close();
            }
            if (insertIntoAlbums != null) {
                insertIntoAlbums.close();
            }
            if (insertIntoSongs != null) {
                insertIntoSongs.close();
            }
            if (queryArtist != null) {
                queryArtist.close();
            }
            if (queryAlbum != null) {
                queryAlbum.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println("Connection is not Closed " + e.getMessage());
        }
    }

    public List<Artist> artistList(int OrderBy) {

        StringBuilder stringBuilder = new StringBuilder(QUERY_ARTIST_ALBUMS_LIST);
        if (OrderBy != NO_ORDER) {
            stringBuilder.append(QUERY_ARTIST_ALBUMS_LIST_SORT);
            if (OrderBy == ASC_ORDER) {
                stringBuilder.append(" ASC ");
            } else {
                stringBuilder.append(" DESC ");
            }
        }

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(stringBuilder.toString())) {

            List<Artist> artists = new ArrayList<>();
            while (resultSet.next()) {
                Artist artist = new Artist();
                artist.setId(resultSet.getInt(INDEX_ARTIST_ID));
                artist.setName(resultSet.getString(INDEX_ARTIST_NAME));
                artists.add(artist);
            }
            return artists;

        } catch (SQLException e){
            System.out.println("failed to run Query " +e.getMessage());
            return null;
        }
    }

    public List<String> artistsAlbums(String artistName, int orderBy) {
        StringBuilder stringBuilder = new StringBuilder(QUERY_ARTIST_LIST);
        stringBuilder.append(artistName);
        stringBuilder.append("\"");

        /*stringBuilder.append(TABLE_ALBUMS);
        stringBuilder.append(".");
        stringBuilder.append(COLUMN_ALBUM_NAME);
        stringBuilder.append(" FROM ");
        stringBuilder.append(TABLE_ALBUMS);
        stringBuilder.append(" INNER JOIN ");
        stringBuilder.append(TABLE_ARTISTS);
        stringBuilder.append(" ON ");
        stringBuilder.append(TABLE_ALBUMS);
        stringBuilder.append(".");
        stringBuilder.append(COLUMN_ALBUM_ARTIST);
        stringBuilder.append(" = ");
        stringBuilder.append(TABLE_ARTISTS);
        stringBuilder.append(".");
        stringBuilder.append(COLUMN_ARTIST_ID);
        stringBuilder.append(" WHERE ");
        stringBuilder.append(TABLE_ARTISTS);
        stringBuilder.append(".");
        stringBuilder.append(COLUMN_ARTIST_NAME);
        stringBuilder.append(" = \"");
        stringBuilder.append(artistName);
        stringBuilder.append("\"");*/

        if (orderBy != NO_ORDER) {
            stringBuilder.append(QUERY_ARTIST_LIST_SORT);

            /*stringBuilder.append(TABLE_ALBUMS);
            stringBuilder.append(".");
            stringBuilder.append(COLUMN_ALBUM_NAME);
            stringBuilder.append(" COLLATE NOCASE");*/

            if (orderBy == ASC_ORDER) {
                stringBuilder.append(" ASC");
            } else {
                stringBuilder.append(" DESC");
            }
        }

        System.out.println(stringBuilder);

        try (Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(stringBuilder.toString())) {

            List<String> albums = new ArrayList<>();
             while (resultSet.next()) {
                 albums.add(resultSet.getString(1));
             }

             return albums;

        } catch (SQLException e) {
            System.out.println("Failed to Run Query " +e.getMessage());
            return null;
        }
    }

    public List<ArtistSongs> artistSongsSearch(String songName, int orderBy) {
        StringBuilder stringBuilder = new StringBuilder(QUERY_ARTIST_SONGS);
        stringBuilder.append(songName);
        stringBuilder.append("\"");

        if (orderBy != NO_ORDER) {
            stringBuilder.append(QUERY_ARTIST_SONGS_SORT);

            if (orderBy == ASC_ORDER) {
                stringBuilder.append("ASC");
            } else {
                stringBuilder.append("DESC");
            }
            System.out.println(stringBuilder);
        }

        return artistSongsResults(stringBuilder);
    }

    public void setQuerySongMetaData() {
        String queryMetaData = "SELECT * FROM " + TABLE_SONGS;

        try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(queryMetaData)) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            int numOfColumns = resultSetMetaData.getColumnCount();
            System.out.println("The Number of Columns For Table Songs: " + numOfColumns);

            for (int i = 1; i <= numOfColumns; i++) {
                System.out.println("The Column " + i + "in Table Songs has the Name: "
                +resultSetMetaData.getColumnName(i));
            }
        } catch (SQLException exception) {
            System.out.println("Failed to Run Query: " + exception.getMessage());
        }
    }

    public int getCount(String tableName) {
        String queryStringCount = "SELECT COUNT(*) FROM " + tableName;
         try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(queryStringCount)) {
             return resultSet.getInt(1);
         } catch (SQLException exception) {
             System.out.println("Failed To Get Count " + exception.getMessage());
             return -1;
         }
    }

    public boolean createViewForSongArtists() {
        try (Statement statement = connection.createStatement()) {
            statement.execute(CREATE_ARTISTS_SONG_VIEW);
            return true;
        } catch (SQLException exception) {
            System.out.println("Failed to Create the View " + exception.getMessage());
            return false;
        }
    }

    public List<ArtistSongs> querySongTitleInfoView(String songTitle) {
        /*StringBuilder stringBuilder = new StringBuilder(QUERY_SONG_TITLE_INFO_VIEW);
        stringBuilder.append(songTitle);
        stringBuilder.append("\"");

        return artistSongsResults(stringBuilder);*/

        try {
            querySongTitleInfoViewStatement.setString(1, songTitle);
            ResultSet resultSet = querySongTitleInfoViewStatement.executeQuery();

            List<ArtistSongs> artistSongs = new ArrayList<>();
            while (resultSet.next()) {
                ArtistSongs artistSong = new ArtistSongs();
                artistSong.setArtistName(resultSet.getString(1));
                artistSong.setAlbumName(resultSet.getString(2));
                artistSong.setTrack(resultSet.getInt(3));
                artistSongs.add(artistSong);
            }
            return artistSongs;
        } catch (SQLException exception) {
            System.out.println("Failed to get the View " + exception.getMessage());
            return null;
        }
    }

    private List<ArtistSongs> artistSongsResults(StringBuilder stringBuilder) {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(stringBuilder.toString())) {
            List<ArtistSongs> artistSongs =  new ArrayList<>();
            while (resultSet.next()) {
                ArtistSongs artistSong = new ArtistSongs();
                artistSong.setArtistName(resultSet.getString(1));
                artistSong.setAlbumName(resultSet.getString(2));
                artistSong.setTrack(resultSet.getInt(3));
                artistSongs.add(artistSong);
            }
            return artistSongs;
        } catch (SQLException exception) {
            System.out.println("Failed to get the View " + exception.getMessage());
            return null;
        }
    }

    private int insertArtists(String name) throws SQLException{
        queryArtist.setString(1, name);
        ResultSet resultSet = queryArtist.executeQuery();

        if (resultSet.next()){
            return resultSet.getInt(1);
        }else {
            insertIntoArtists.setString(1,name);
            int numAffectedRows = insertIntoArtists.executeUpdate();

            if (numAffectedRows != 1){
                throw new SQLException("Couldn't insert the artist");
            }
            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if (generatedKeys.next()){
                return generatedKeys.getInt(1);
            }else {
                throw new SQLException("Couldn't get id for artist " + name);
            }
        }
    }

    private int insertAlbums(String albumName, int artistId) throws SQLException {
        queryAlbum.setString(1, albumName);
        ResultSet resultSet = queryAlbum.executeQuery();

        if (resultSet.next()) {
            return resultSet.getInt(1);
        } else {
            insertIntoAlbums.setString(1, albumName);
            insertIntoAlbums.setInt(2, artistId);

            int numAffectedRows = insertIntoAlbums.executeUpdate();

            if (numAffectedRows != 1) {
                throw new SQLException("Could not Insert the Album " + albumName);
            }
            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();

            if (generatedKeys.next()) {
                return generatedKeys.getInt(1);
            } else {
                throw new SQLException("Could not get Artist_Id for album " + albumName);
            }
        }
    }

    public void insertSongs(String songTitle, String artistName, String albumName, int track) {
        try {
           connection.setAutoCommit(false);
           int artistId = insertArtists(artistName);
           int albumId = insertAlbums(albumName, artistId);
           insertIntoSongs.setInt(1, track);
           insertIntoSongs.setString(2, songTitle);
           insertIntoSongs.setInt(3, albumId);

           int numAffectedRows = insertIntoSongs.executeUpdate();

           if (numAffectedRows == 1) {
               connection.commit();
           } else {
               throw new SQLException("Failed to insert Song " + songTitle);
           }
        } catch (SQLException exception) {
            System.out.println("Failed to insert Song exception " + exception.getMessage());

            try {
                System.out.println("connection.rollback in action");
                connection.rollback();
            } catch (SQLException ex) {
                System.out.println("Unable to RollBack" + ex.getMessage());
            }
        } finally {
            try {
                System.out.println("Committing the changes, and setting to true");
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                System.out.println("Could not set AuoCommit to True " + e.getMessage());
            }
        }
    }

}

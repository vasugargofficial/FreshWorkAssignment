import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

public class FileBasedKeyValueDB {

    private String db_path;
    private JSONArray objectsList;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private FileLock lock;

    public FileBasedKeyValueDB() throws IOException, FileLockedException, FileSizeLimitedException {
        String curPath = System.getProperty("user.dir");
        db_path = curPath;
        initializeObjectsList(db_path);
    }

    public FileBasedKeyValueDB(String custom_path) throws IOException, FileLockedException, FileSizeLimitedException {
        db_path = custom_path;
        initializeObjectsList(db_path);
    }

    private boolean isKeyPresent(String key) {

        JSONObject data = null;
        int flag = 0;
        for (Object data2 : objectsList) {
            data = (JSONObject) data2;
            for (Object keyStr : data.keySet()) {
                if (keyStr.equals(key)) {
                    flag = 1;
                    break;
                }

            }
        }
        return flag != 0;
    }

    private void initializeObjectsList(String db_path) throws IOException, FileSizeLimitedException {


        File dir = new File(db_path);
        if (!dir.exists()) {
            dir.mkdirs();
            File file = new File(db_path + "/db.json");
            file.createNewFile();
            objectsList = new JSONArray();
        } else {
            File file = new File(db_path + "/db.json");
            if (!file.exists()) {
                file.createNewFile();
                objectsList = new JSONArray();
            } else {

                if(file.length()<=1073741824) {
                    try (FileReader reader = new FileReader(db_path + "/db.json")) {

                        if (file.length() != 0) {
                            JSONParser jsonParser = new JSONParser();
                            Object obj = jsonParser.parse(reader);
                            objectsList = (JSONArray) obj;

                        } else {
                            objectsList = new JSONArray();
                        }

                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                }
                else throw new FileSizeLimitedException("File sized is reached to limit of 1gb");
            }
        }


    }

    private boolean isFileLocked() {
        try {
            randomAccessFile = new RandomAccessFile(db_path + "/db.json", "rw");
            fileChannel = randomAccessFile.getChannel();
            lock = null;
            try {
                lock = fileChannel.tryLock();
            } catch (final OverlappingFileLockException e) {
                randomAccessFile.close();
                fileChannel.close();
            }
            return lock == null;
        } catch (Exception e) {
            e.printStackTrace();
            return true;
        }
    }

    public void create(String key, JSONObject jsonObject) throws KeyAlreadyExistsException, KeyLengthException, JSONSizeException {

        if(key.length()<=32 && jsonObject.toString().getBytes().length <=16000) {
            jsonObject.put("timetolive", Integer.toString(Integer.MAX_VALUE));
            LocalDateTime now = LocalDateTime.now();
            jsonObject.put("createdAt", now.toString());
            addNewPair(key, jsonObject);
        }
        else if(key.length()>32 ){
            throw new KeyLengthException("key length should not be greater than 32 chars");
        }
        else if(jsonObject.toString().getBytes().length >16000){
            throw new JSONSizeException("JSON Object size is greater than 16kb");
        }

    }

    public void create(String key, JSONObject jsonObject, int timeToLive) throws KeyAlreadyExistsException, KeyLengthException, JSONSizeException {

        if(key.length()<=32 && jsonObject.toString().getBytes().length <=16) {
            jsonObject.put("timetolive", Integer.toString(timeToLive));
            LocalDateTime now = LocalDateTime.now();
            jsonObject.put("createdAt", now.toString());
            addNewPair(key, jsonObject);
        }
        else if(key.length()>32 ){
            throw new KeyLengthException("key length should not be greater than 32 chars");
        }
        else if(jsonObject.toString().getBytes().length >16000){
            throw new JSONSizeException("JSON Object size is greater than 16kb");
        }

    }

    private void addNewPair(String key, JSONObject jsonObject) throws KeyAlreadyExistsException {

        if (!isKeyPresent(key)) {
            try {
                if (!isFileLocked()) {
                    JSONObject new_pair = new JSONObject();
                    new_pair.put(key, jsonObject);
                    objectsList.add(new_pair);
                    FileWriter file = new FileWriter(db_path + "/db.json");
                    TimeUnit.MINUTES.sleep(1);
                    file.write(objectsList.toJSONString());
                    file.close();
                    System.out.println("Data Added Successfully");
                    unlockFile();
                } else throw new FileLockedException("File is already in use by other process");
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else throw new KeyAlreadyExistsException("Try again with new key");
    }

    public JSONObject readJSONObject(String key) throws KeyNotFoundException {
        JSONObject queriedObject = null;
        try {
            if (!isFileLocked()) {
                checkTimeToLive();

                if (isKeyPresent(key)) {
                    for (Object object : objectsList) {
                        JSONObject jsonObject = (JSONObject) object;
                        for (Object keyStr : jsonObject.keySet()) {
                            if (keyStr.equals(key)) {
                                queriedObject = jsonObject;
                            }
                        }
                    }
                    unlockFile();
                    return queriedObject;
                } else {
                    unlockFile();
                    throw new KeyNotFoundException("'" + key + "'" + " is not found in DataStore");
                }
            } else throw new FileLockedException("File is already in use by other process");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queriedObject;
    }

    private void deleteJSONObject(String key) throws KeyNotFoundException {

        if (isKeyPresent(key)) {
            for (int i = 0; i < objectsList.size(); i++) {

                JSONObject jsonObject = (JSONObject) objectsList.get(i);
                for (Object keyStr : jsonObject.keySet()) {
                    if (keyStr.equals(key)) {
                        // System.out.println(jsonObject);
                        objectsList.remove(jsonObject);

                        try {
                            FileWriter file = new FileWriter(db_path + "/db.json");
                            file.write(objectsList.toJSONString());
                            file.close();
                            // System.out.println("File deleted Successfully");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        break;

                    }
                }
            }
        } else
            throw new KeyNotFoundException("'" + key + "'" + " is not found in DataStore");
    }

    private void unlockFile() throws IOException {
        if (lock != null) {
            lock.release();
            randomAccessFile.close();
            fileChannel.close();
        }
    }

    private void checkTimeToLive() throws KeyNotFoundException {


        for (int i = 0; i < objectsList.size(); i++) {

            JSONObject jsonObject = (JSONObject) objectsList.get(i);
            for (Object keyStr : jsonObject.keySet()) {


                JSONObject jsonObject2 = (JSONObject) jsonObject.get(keyStr);
                long timetolive = Long.parseLong(jsonObject2.get("timetolive").toString());
                LocalDateTime creationTime = LocalDateTime.parse((jsonObject2.get("createdAt").toString()));
                LocalDateTime currentTime = LocalDateTime.now();

                long difference = creationTime.until(currentTime, ChronoUnit.SECONDS);

                if (difference >= timetolive)
                    delete(keyStr.toString(), true);
            }
        }


    }

    public void delete(String key) throws KeyNotFoundException {
        try {
            if (!isFileLocked()) {
                deleteJSONObject(key);
                System.out.println("Data deleted Successfully");
                unlockFile();
            } else throw new FileLockedException("File is already in use by other process");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void delete(String key, boolean timetolive) throws KeyNotFoundException {

        try {
            if (!isFileLocked()) {
                deleteJSONObject(key);
                unlockFile();
            } else throw new FileLockedException("File is already in use by other process");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

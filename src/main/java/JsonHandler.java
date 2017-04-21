import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by Daniel on 2016-12-07.
 */
public class JsonHandler {

    int n =0;
    SQLquery sql = new SQLquery();
    JSONParser parser = new JSONParser();
    String[] matches = new String[] {
            "id",
            "parent_id",
            "link_id",
            "name",
            "author",
            "body",
            "subreddit_id",
            "subreddit",
            "score",
            "created_utc"};

    public JsonHandler() throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException {
    }

    public void parseJson(String url) {
        boolean b = false;
        try {
            File file = new File(url);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(file));

            while ((line = br.readLine()) != null) {
                Object obj = parser.parse(line);
                JSONObject jsonObject = (JSONObject) obj;

                String[] jsonToSave = new String[10];
                jsonToSave[0] = jsonObject.get("id").toString();
                jsonToSave[1] = jsonObject.get("parent_id").toString();
                jsonToSave[2] = jsonObject.get("link_id").toString();
                jsonToSave[3] = jsonObject.get("name").toString();
                jsonToSave[4] = jsonObject.get("author").toString();
                jsonToSave[5] = jsonObject.get("body").toString();
                jsonToSave[6] = jsonObject.get("subreddit_id").toString();
                jsonToSave[7] = jsonObject.get("subreddit").toString();
                jsonToSave[8] = jsonObject.get("score").toString();
                jsonToSave[9] = jsonObject.get("created_utc").toString();
                /*
                if (!b) {
                    for (String s: jsonToSave) {
                        System.out.println(s);
                    }
                    b = true;
                }
                if (jsonToSave[5].length() > 1000) {
                    System.out.println("Length: " + jsonToSave[5].length());
                    System.out.println(jsonToSave[5]);
                }
                */
                sql.addDbEntry(jsonToSave);
                n++;
                /*
                String[] entryLine = line.split(",");
                for (String s : entryLine) {
                    for (String m : matches) {
                        if (s.contains(m)) {
                            s = s.replaceAll(".*:", "");
                            sql.addDbEntry(entryLine);
                            n++;
                        }
                    }
                }
                */
            }
            sql.connection.commit();
            System.out.println();
            System.out.println("Lines saved: " + n);
        }
        catch (Exception e) {
            System.out.println("Parse error");
            e.printStackTrace();
        }
    }
}

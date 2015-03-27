/**
 * Created by evo on 2/13/15.
 *
 * This utility is used as singleton interface to the MaxMind API and database. It also casches the MaxMind Database
 * (by still using the MaxMind API). Using MaxMind as implemented here is thread-safe.
 *
 * It ensures that MaxMind database is deployed and casched in-memory on every Spark Worker Node and hence used locally
 * and simultaneously by all parallel Tasks running in a Worker Node.
 *
 */

import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

public class MaxMindSingleton {

    private static final MaxMindSingleton instance = new MaxMindSingleton();

    private DatabaseReader reader;

    private MaxMindSingleton(){

        //ToDO: get this param either from the env of the Worker Node or sent by the Driver
        File database = new File("/home/faganpe/spark-addons/MaxMind/GeoLite2-Country.mmdb");

        try {

            //casche the MaxMind database in the memory of the Spark Worker Node
            reader = new DatabaseReader.Builder(database).fileMode(Reader.FileMode.MEMORY).build();

        }catch (IOException e)
        {
            //print the stack trace to the error log of the Worker Node
            e.printStackTrace(System.err);

        }


    }

    public static MaxMindSingleton getInstance(){
        return instance;
    }


    public String getCountry(String ipAddr) {

        Country country = null;

        try {

            InetAddress ipAddress = InetAddress.getByName(ipAddr);

            CountryResponse response = reader.country(ipAddress);

            if (response == null)
                return "NO_COUNTRY_FOUND";

            country = response.getCountry();

        }catch (IOException e)
        {
            //print the stack trace to the error log of the Worker Node
            e.printStackTrace(System.err);

        }
        catch (GeoIp2Exception e)
        {
            //print the stack trace to the error log of the Worker Node
            e.printStackTrace(System.err);
            country = null;
        }
        finally {
            if (country == null) {
                return "NO_COUNTRY_FOUND";
            }
            else {
                return country.getName();
            }
        }


    }


}

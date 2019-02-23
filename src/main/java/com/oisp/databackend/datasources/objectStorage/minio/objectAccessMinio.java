package com.oisp.databackend.datasources.objectStorage.minio;

import com.oisp.databackend.config.oisp.OispConfig;
import com.oisp.databackend.datasources.objectStorage.ObjectStoreAccess;
import com.oisp.databackend.datastructures.Observation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.net.URISyntaxException;
import java.util.List;

@Repository
public class objectAccessMinio implements ObjectStoreAccess {

    @Autowired
    private OispConfig oispConfig;

    @PostConstruct
    public void init() throws URISyntaxException {
        //Make sure that this bean is only initiated when needed
        if (oispConfig.getBackendConfig().getObjectStoreName() == null ||
                !oispConfig.getBackendConfig().getObjectStoreName().equals(oispConfig.OISP_BACKEND_OBJECT_STORE_MINIO)) {
            return;
        }
        //otherwise do the init here
    }

    @Override
    public boolean put(Observation observation){
        return true;
    }

    @Override
    public boolean put(List<Observation> observationList){

        return true;
    }
    @Override
    public void get(Observation[] observations){

    }
}

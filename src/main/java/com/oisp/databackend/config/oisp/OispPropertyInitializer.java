package com.oisp.databackend.config.oisp;

import com.oisp.databackend.exceptions.ConfigEnvironmentException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class OispPropertyInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

private static String CUSTOM_PREFIX = "backendConfig.";

    BackendConfig backendConfig;
    @Override
    @SuppressWarnings("unchecked")
    public void
    initialize(ConfigurableApplicationContext configurableApplicationContext) {
        //introspect((Object)backendConfig, CUSTOM_PREFIX);
        try {
            backendConfig = new OispConfig().init();
        } catch (ConfigEnvironmentException e) {
            throw new Error("Cannot initialize properties: " + e);
        }
        scanAndExposeObject(configurableApplicationContext, backendConfig, CUSTOM_PREFIX);

    }
    void scanAndExposeObject(ConfigurableApplicationContext cac, Object o, String prefix) {
        Field[] fields = o.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);
            MapPropertySource propertySource;
            try {
                if (field.getType().getName() == String.class.getTypeName()
                        || field.getType().getName() == int.class.getTypeName()
                        || field.getType().getName() == Number.class.getTypeName()
                        || field.getType().getName() == boolean.class.getTypeName()
                        || field.getType().getName() == Properties.class.getTypeName()) {


                    propertySource = new MapPropertySource(
                            prefix + field.getName(),
                            Collections.singletonMap(
                                    prefix + field.getName(), field.get(o)
                            ));
                    cac.getEnvironment()
                            .getPropertySources()
                            .addFirst(propertySource);

                } else {
                    scanAndExposeObject(cac, field.get(o), prefix + field.getName() + ".");
                }
            } catch (IllegalAccessException e) {
                throw new Error("Could not Introspect Object: " + e);
            }

        }
    }
}

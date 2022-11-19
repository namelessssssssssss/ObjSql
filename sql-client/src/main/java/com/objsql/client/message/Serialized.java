package com.objsql.client.message;

import java.io.Serializable;

public interface Serialized extends Serializable {

    byte[] serialize();

}

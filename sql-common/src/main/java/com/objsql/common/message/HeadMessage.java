package com.objsql.common.message;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
public class HeadMessage {
    private final Integer magic;
    private final Byte version;
    private final Byte serializeType;
    private final Integer sequenceId;
    private final Byte messageType;
    private final Integer bodyLen;
}

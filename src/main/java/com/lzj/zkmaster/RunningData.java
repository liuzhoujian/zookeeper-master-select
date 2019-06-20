package com.lzj.zkmaster;

import lombok.Data;

import java.io.Serializable;

/**
 * 服务器数据
 */
@Data
public class RunningData implements Serializable {
    private Long cid;
    private String name;
}

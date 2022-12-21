package com.ww.es.base;


import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;

/**
 * 基本结果
 *
 * @author weiwang127
 */
@Data
@ApiModel("接口返回结果")
public class BaseResult<T> implements Serializable {
    /** 业务是否正常 */
    @ApiModelProperty("业务是否正常")
    private boolean flag;
    @ApiModelProperty("code")
    private Integer code;
    @ApiModelProperty("异常消息")
    private String message;
    @ApiModelProperty("返回结果")
    private T data;

    public static <T> BaseResult<T> success(T data) {
        BaseResult<T> ret = new BaseResult<>();
        ret.flag = true;
        ret.code = CODES.OK.code;
        ret.message = CODES.OK.msg;
        ret.data = data;
        return ret;
    }
    public static <T> BaseResult<T> success(String msg,T data) {
        BaseResult<T> ret = new BaseResult<>();
        ret.flag = true;
        ret.code = CODES.OK.code;
        ret.message = msg;
        ret.data = data;
        return ret;
    }
    public static <T> BaseResult<T> success() {
        BaseResult<T> ret = new BaseResult<>();
        ret.flag = true;
        ret.code = CODES.OK.code;
        ret.message = CODES.OK.msg;
        return ret;
    }

    public static <T> BaseResult<T> fail(String msg) {
        BaseResult<T> ret = new BaseResult<>();
        ret.flag = false;
        ret.code = CODES.FAIL.code;
        ret.message = msg;
        return ret;
    }

    public static <T> BaseResult<T> fail(Integer code,String msg, T data) {
        BaseResult<T> ret = new BaseResult<>();
        ret.flag = false;
        ret.code = code;
        ret.message = msg;
        ret.data = data;
        return ret;
    }


    public static <T> BaseResult<T> fail(String msg, T data) {
        BaseResult<T> ret = new BaseResult<>();
        ret.flag = false;
        ret.code = CODES.FAIL.code;
        ret.message = msg;
        ret.data = data;
        return ret;
    }

    public static <T> BaseResult<T> fail() {
        BaseResult<T> ret = new BaseResult<>();
        ret.flag = false;
        ret.code = CODES.FAIL.code;
        ret.message = CODES.FAIL.msg;
        return ret;
    }

    @Getter
    @AllArgsConstructor
    public enum CODES {
        /**
         * 成功
         */
        OK(0, "成功"),
        /**
         * 失败
         */
        FAIL(999, "业务错误");
        private final Integer code;
        private final String msg;
    }
}

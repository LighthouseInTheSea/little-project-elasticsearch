package com.ruyuan.little.project.elasticsearch.biz;

import com.ruyuan.little.project.common.dto.CommonResponse;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

    @RequestMapping("/")
    public CommonResponse health(){
       return CommonResponse.success();
    }
}

package com.tipdm.controller;

import com.tipdm.service.UserLabelService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

/**
 * Created by ch on 2018/10/31
 */
@Controller
@RequestMapping("/labels")
public class UserLabelController {
    private final static Logger logger = LoggerFactory.getLogger(UserLabelController.class);
    @Autowired
    private UserLabelService userLabelService;

    @GetMapping("/user/{id}")
    @ResponseBody
    public List getUserLabelById(@PathVariable(name = "id") Long id) {
        return userLabelService.getLabelsByPhoneNo(id);
    }

    @GetMapping("/samples")
    @ResponseBody
    public List<Long> getSampleUsers() {
        return userLabelService.getSamplePhoneNos();
    }

    @GetMapping("/users/{id}/all/")
    @ResponseBody
    public HttpEntity getLabelController(@PathVariable(name = "id") Long id, HttpServletRequest request, HttpServletResponse response) {
        Result result = new Result();
        try {
            Map<String, Object> map = userLabelService.findLabel(id);
            result.setData(map);
        } catch (Exception e) {
            result.setMessage(e.getMessage());
            result.setStatus(Result.Status.FAIL);
        }
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json;charset=UTF-8");
        ResponseEntity responseEntity = new ResponseEntity(result, headers, HttpStatus.valueOf(response.getStatus()));
        return responseEntity;
    }

}

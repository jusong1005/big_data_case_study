package com.tipdm.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import javax.servlet.http.HttpSession;

/**
 * Created by ch on 2018/11/5
 */
@Controller
public class HomeController {

    @GetMapping("/")
    public String index(Model model, HttpSession session) {
        return "index";
    }


}

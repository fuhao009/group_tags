package com.study.sparkweb.sparkweb;


import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

@RestController
public class HelloWeb {
    @RequestMapping(value = "hello",method = RequestMethod.GET)
    public String hello(){
        return "hellospoot";
    }
    @RequestMapping(value = "firedamp",method = RequestMethod.GET)
    public ModelAndView test()
    {
        System.out.println("1231231");
        return  new ModelAndView("test");
    }
}

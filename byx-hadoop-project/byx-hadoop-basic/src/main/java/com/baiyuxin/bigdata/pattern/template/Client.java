package com.baiyuxin.bigdata.pattern.template;


public class Client {

    public static void main(String[] args) {

        Mapper pkMapper = new PKMapper();
        pkMapper.run();

        System.out.println("-------------");

        Mapper ruozedataMapper = new RuozedataMapper();
        ruozedataMapper.run();
    }
}

package com.dataiku.dip.plugins.anonymizer;

import org.python.google.common.collect.Lists;

import java.util.List;

public enum StandardDict {
    CUSTOM(null),
    STAR_WARS_PLANETS(new String[]{
            "Utapau",
            "Mustafar",
            "Kashyyyk",
            "Polis Massa",
            "Mygeeto",
            "Felucia",
            "Cato Neimoidia",
            "Saleucami",
            "Stewjon",
            "Eriadu",
            "Corellia",
            "Rodia",
            "Nal Hutta",
            "Dantooine",
            "Bestine IV",
            "Ord Mantell",
            "Trandosha",
            "Socorro",
            "Mon Cala",
            "Chandrila",
            "Sullust",
            "Toydaria",
            "Malastare",
            "Dathomir",
            "Ryloth",
            "Aleen Minor",
            "Vulpter",
            "Troiken",
            "Tund",
            "Haruun Kal",
            "Cerea",
            "Glee Anselm",
            "Iridonia",
            "Tholoth",
            "Iktotch",
            "Quermia",
            "Dorin",
            "Champala",
            "Mirial",
            "Serenno",
            "Concord Dawn",
            "Zolan",
            "Ojom",
            "Skako",
            "Muunilinst",
            "Shili",
            "Kalee",
            "Umbara",
            "Tatooine",
            "Jakku",
            "Alderaan",
            "Yavin IV",
            "Hoth",
            "Dagobah",
            "Bespin",
            "Endor",
            "Naboo",
            "Coruscant",
            "Kamino",
            "Geonosis",
            "Nar Shadaa",
            "Korriban"
    }),
    
    DWARF_NAMES(new String[]{
            "Ulfarur", "Dwalilin", "Bomban", "Nôrilin", "Korin ", "Far﻿in", "Gloek", "Danund", "Borador", "Brogin", "Dain ", "Gloin", "Ulfari", "Nargïn", "Dolgaran", "Nôröm", "Daöm", "Gimlilin", "Thorin", "Daror", "Nôrin ", "Forbilin", "Tarköm", "Tharok", "Fari", "Ulfor", "Bor﻿in", "Brelir", "Dwalöm", "Glound", "Bardur", "Forbir", "Daïn", "Darond", "Thoran", "Kazadrin", "Darinn", "Dworken", "Farïn", "Danir", "Balir", "Narvin", "Bofïn", "Boraden", "Danöm", "Duröm", "Gimlan", "Fili", "Duerinn", "Thorek",
            "Gimlir", "Dueröm", "Boror", "Nargan", "Fraim", "Torrund", "Dworkond", "Orror", "Errur", "Kurïn", "Bukok", "Kilin ", "Brogan", "Forbinn", "Dworköm", "Ulfi", "Ulfarok", "Borïn", "Dwilor", "Kazadrilin", "Alabri", "Dwilim", "Theror", "Murim", "Nurri", "Nargïn", "Dueri", "Ulfinn", "Kirek", "Tarkur", "Ulfarin ", "Dain ", "Bardilin", "Bardund", "Nurrir", "Gloinn", "Barden", "Kazadrur", "Darilin", "Murilin", "Nurrim", "Fraïn", "Farïn", "Orröm", "Boradok", "Korben", "Dwilin ", "Moradïn", "Gimlin", "Nurrur"
    });
    
    

    StandardDict(String[] data) {
        this.data = data;
    }
    private String[] data;

    public List<String> getData(){
        return Lists.newArrayList(data);
    }
}

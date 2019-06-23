package scut.se.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.*;
import scut.se.dbutils.Tuple;
import scut.se.entity.PageInfo;
import scut.se.search.SearchUtil;

import java.util.ArrayList;
import java.util.List;

@Controller
public class IndexController {

    class SearchEntry {

        private String searchText;

        public SearchEntry() {
            this.searchText = "search";
        }

        public String getSearchText() {
            return searchText;
        }

        public void setSearchText(String searchText) {
            this.searchText = searchText;
        }
    }

    @GetMapping("/index")
    public String index(Model model) {
        return "index";
    }

    @PostMapping("/search")
    public String search(@ModelAttribute SearchEntry se, Model model) {
        List<Tuple<PageInfo, Integer>> resultList = SearchUtil.getResult(se.searchText);
        List<PageInfo> showList = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            showList.add(resultList.get(i)._1());
        }
        model.addAttribute("showList", showList);
        return "result";
    }
}

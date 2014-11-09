package com.googlecode.kanbanik.client.components.filter;

import com.google.gwt.i18n.client.DateTimeFormat;
import com.google.gwt.regexp.shared.RegExp;
import com.google.gwt.storage.client.Storage;
import com.google.gwt.user.client.ui.HTML;
import com.googlecode.kanbanik.client.api.DtoFactory;
import com.googlecode.kanbanik.client.api.Dtos;
import com.googlecode.kanbanik.client.managers.UsersManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class BoardsFilter {

    private static final String KEY = "FILTER_DATA_KEY";

    private static final Storage storage = Storage.getLocalStorageIfSupported();

    public static final int DATE_CONDITION_UNSET = 0;
    public static final int DATE_CONDITION_LESS = 1;
    public static final int DATE_CONDITION_EQALS = 2;
    public static final int DATE_CONDITION_MORE = 3;
    public static final int DATE_CONDITION_BETWEEN = 4;

    private Dtos.FilterDataDto filterDataDto;

    public boolean taskMatches(Dtos.TaskDto task) {

        List<Dtos.FilteredEntity> filteredEntities = filterDataDto.getFullTextFilter().getFilteredEntities();

        if (filteredEntities != null && filteredEntities.size() != 0) {
            boolean matches = false;

            if (filteredEntities.contains(Dtos.FilteredEntity.SHORT_DESCRIPTION)) {
                if (stringMatches(filterDataDto.getFullTextFilter(), task.getName())) {
                    matches = true;
                }
            }

            if (!matches && filteredEntities.contains(Dtos.FilteredEntity.LONG_DESCRIPTION)) {
                if (stringMatches(filterDataDto.getFullTextFilter(), new HTML(task.getDescription()).getText())) {
                    matches = true;
                }
            }

            if (!matches && filteredEntities.contains(Dtos.FilteredEntity.TICKET_ID)) {
                if (stringMatches(filterDataDto.getFullTextFilter(), task.getTicketId())) {
                    matches = true;
                }
            }

            if (!matches) {
                return false;
            }
        }

        if (!checkDueDates(task)) {
            return false;
        }

        int classOfServicePosition = task.getClassOfService() == null ? -1 : findById(task.getClassOfService());

        boolean classOfServiceMatches = task.getClassOfService() != null && classOfServicePosition != -1 && filterDataDto.getClassesOfServices().get(classOfServicePosition).isSelected();
        if (!classOfServiceMatches) {
            // still can be the default
            if (task.getClassOfService() == null && defaultClassOfServiceSelected()) {
                return true;
            }

            // naaa, not this time
            return false;
        }

        int assigneePosition = findById(task.getAssignee());

        boolean userMatches = assigneePosition != -1 && filterDataDto.getUsers().get(assigneePosition).isSelected();

        return userMatches;
    }

    private boolean defaultClassOfServiceSelected() {
        for (Dtos.ClassOfServiceWithSelectedDto classOfServiceWithSelectedDto : filterDataDto.getClassesOfServices()) {
            if (classOfServiceWithSelectedDto.getClassOfService().getId() == null) {
                return classOfServiceWithSelectedDto.isSelected();
            }
        }

        return false;
    }

    public Date parseDate(String date) {
        if (date == null || "".equals(date)) {
            return null;
        }

        try {
            return DateTimeFormat.getFormat(DateTimeFormat.PredefinedFormat.DATE_SHORT).parse(date);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private boolean checkDueDates(Dtos.TaskDto task) {
        if (filterDataDto.getDueDate() == null) {
            return true;
        }

        int dateCondition = filterDataDto.getDueDate().getCondition();

        if (dateCondition == DATE_CONDITION_UNSET) {
            return true;
        }

        // task does not have a due date and I'm interested in some particular one
        if (task.getDueDate() == null || "".equals(task.getDueDate())) {
            return false;
        }

        Date dateFrom = parseDate(filterDataDto.getDueDate().getDateFrom());
        if (dateFrom == null) {
            // incorrectly set - show everything
            return true;
        }

        Date dateTo = parseDate(filterDataDto.getDueDate().getDateTo());
        if (dateCondition == DATE_CONDITION_BETWEEN && dateTo == null) {
            // incorrectly set - show everything
            return true;
        }

        Date taskDueDate = null;
        try {
            taskDueDate = DateTimeFormat.getFormat(DateTimeFormat.PredefinedFormat.DATE_SHORT).parse(task.getDueDate());
        } catch (IllegalArgumentException e) {
            // wrongly set due date on task - ignore it
            return false;
        }

        if (dateCondition == DATE_CONDITION_LESS) {
            return taskDueDate.before(dateFrom);
        }

        if (dateCondition == DATE_CONDITION_MORE) {
            return taskDueDate.after(dateFrom);
        }

        if (dateCondition == DATE_CONDITION_EQALS) {
            return taskDueDate.equals(dateFrom);
        }

        if (dateCondition == DATE_CONDITION_BETWEEN) {
            return taskDueDate.after(dateFrom) && taskDueDate.before(dateTo);
        }

        return true;

    }

    private boolean stringMatches(Dtos.FullTextMatcherDataDto pattern, String real) {
        boolean patternEmpty = pattern == null || pattern.getString() == null;
        boolean realEmpty = real == null || "".equals(real);

        if (patternEmpty) {
            return true;
        }

        String actual = real;
        String expected = pattern.getString();

        boolean matches = false;

        if (pattern.isRegex()) {
            try {
                RegExp regExp = RegExp.compile(expected);
                matches = regExp.exec(actual) != null;
            } catch (Exception e) {
                // incorrect regex - ignore it and show everything
                matches = true;
            }
        } else {
            if (!pattern.isCaseSensitive()) {
                actual = actual.toLowerCase();
                expected = expected.toLowerCase();
            }
            matches = actual.contains(expected);
        }

        return pattern.isInverse() ? !matches : matches;
    }

    public void add(Dtos.BoardDto boardDto) {
        int id = findById(boardDto);
        if (id == -1) {
            filterDataDto.getBoards().add(DtoFactory.withSelected(boardDto, true));
        } else {
            filterDataDto.getBoards().get(id).setSelected(true);
        }
    }

    public void remove(Dtos.BoardDto boardDto) {
        int id = findById(boardDto);
        if (id != -1) {
            filterDataDto.getBoards().get(id).setSelected(false);
        } else {
            filterDataDto.getBoards().add(DtoFactory.withSelected(boardDto, false));
        }
    }

    public void add(Dtos.ClassOfServiceDto classOfServiceDto) {
        int id = findById(classOfServiceDto);
        if (id == -1) {
            filterDataDto.getClassesOfServices().add(DtoFactory.withSelected(classOfServiceDto, true));
        } else {
            filterDataDto.getClassesOfServices().get(id).setSelected(true);
        }
    }

    public void remove(Dtos.ClassOfServiceDto classOfServiceDto) {
        int id = findById(classOfServiceDto);
        if (id != -1) {
            filterDataDto.getClassesOfServices().get(id).setSelected(false);
        } else {
            filterDataDto.getClassesOfServices().add(DtoFactory.withSelected(classOfServiceDto, false));
        }
    }

    public void add(Dtos.UserDto userDto) {
        int id = findById(userDto);
        if (id == -1) {
            filterDataDto.getUsers().add(DtoFactory.withSelected(userDto, true));
        } else {
            filterDataDto.getUsers().get(id).setSelected(true);
        }
    }

    public void remove(Dtos.UserDto userDto) {
        int id = findById(userDto);
        if (id != -1) {
            filterDataDto.getUsers().get(id).setSelected(false);
        } else {
            filterDataDto.getUsers().add(DtoFactory.withSelected(userDto, false));
        }
    }

    public void add(Dtos.BoardWithProjectsDto entity) {
        int id = findById(entity);
        if (id == -1) {
            filterDataDto.getBoardWithProjectsDto().add(DtoFactory.withSelected(entity, true));
        } else {
            filterDataDto.getBoardWithProjectsDto().get(id).setSelected(true);
        }
    }

    public void remove(Dtos.BoardWithProjectsDto entity) {
        int id = findById(entity);
        if (id != -1) {
            filterDataDto.getBoardWithProjectsDto().get(id).setSelected(false);
        } else {
            filterDataDto.getBoardWithProjectsDto().add(DtoFactory.withSelected(entity, false));
        }
    }

    public boolean isSelected(Dtos.BoardDto boardDto) {
        int id = findById(boardDto);
        return id == -1 || filterDataDto.getBoards().get(id).isSelected();
    }

    public int findById(Dtos.BoardDto boardDto) {
        int id = 0;

        for (Dtos.BoardWithSelectedDto candidate : filterDataDto.getBoards()) {
            if (candidate.getBoard().getId().equals(boardDto.getId())) {
                return id;
            }

            id ++;
        }

        return -1;
    }

    public boolean isSelected(Dtos.UserDto usersDto) {
        int id = findById(usersDto);
        return id == -1 || filterDataDto.getUsers().get(id).isSelected();
    }

    public int findById(Dtos.UserDto userDto) {
        int id = 0;

        Dtos.UserDto toLookFor = userDto != null ? userDto : UsersManager.getInstance().getNoUser();

        for (Dtos.UserWithSelectedDto candidate : filterDataDto.getUsers()) {
            if (candidate.getUser().getUserName().equals(toLookFor.getUserName())) {
                return id;
            }

            id ++;
        }

        return -1;
    }

    public boolean isSelected(Dtos.ClassOfServiceDto classOfServiceDto) {
        int id = findById(classOfServiceDto);
        return id == -1 || filterDataDto.getClassesOfServices().get(id).isSelected();
    }

    public int findById(Dtos.ClassOfServiceDto classOfServiceDto) {
        int id = 0;

        for (Dtos.ClassOfServiceWithSelectedDto candidate : filterDataDto.getClassesOfServices()) {
            if (candidate.getClassOfService().getId() == null && classOfServiceDto.getId() == null) {
                return id;
            }

            if (
                    candidate.getClassOfService().getId() != null && classOfServiceDto.getId() != null &&
                    candidate.getClassOfService().getId().equals(classOfServiceDto.getId())) {
                return id;
            }

            id ++;
        }

        return -1;
    }

    public boolean isSelected(Dtos.BoardWithProjectsDto boardWithProjectsDto) {
        int id = findById(boardWithProjectsDto);
        return id == -1 || filterDataDto.getBoardWithProjectsDto().get(id).isSelected();
    }

    public int findById(Dtos.BoardWithProjectsDto boardWithProjectsDto) {
        int id = 0;

        String boardId = boardWithProjectsDto.getBoard().getId();
        String projectId = boardWithProjectsDto.getProjectsOnBoard().getValues().get(0).getId();

        for (Dtos.BoardWithProjectsWithSelectedDto candidate : filterDataDto.getBoardWithProjectsDto()) {
            if (candidate.getBoardWithProjects().getBoard().getId().equals(boardId) &&
                candidate.getBoardWithProjects().getProjectsOnBoard().getValues().get(0).getId().equals(projectId)
            ) {
                return id;
            }

            id ++;
        }

        return -1;
    }

    public boolean projectOnBoardMatches(Dtos.ProjectDto projectDto, Dtos.BoardDto boardDto) {
        Dtos.BoardWithProjectsDto boardWithProjectsDto = DtoFactory.boardWithProjectsDto();
        boardWithProjectsDto.setBoard(boardDto);
        List<Dtos.ProjectDto> projects = new ArrayList<Dtos.ProjectDto>();
        projects.add(projectDto);
        boardWithProjectsDto.setProjectsOnBoard(DtoFactory.projectsDto(projects));

        int projectOnBoardPosition = findById(boardWithProjectsDto);

        return projectOnBoardPosition != -1 && filterDataDto.getBoardWithProjectsDto().get(projectOnBoardPosition).isSelected();
    }

    public Dtos.FilterDataDto getFilterDataDto() {
        return filterDataDto;
    }

    public void setFilterDataDto(Dtos.FilterDataDto filterDataDto) {
        this.filterDataDto = filterDataDto;
    }

    public void storeFilterData() {
        if (storage == null) {
            return;
        }

        String json = DtoFactory.asRawJson(filterDataDto);
        storage.setItem(KEY, json);
    }

    public Dtos.FilterDataDto loadFilterData() {
        if (storage == null) {
            return null;
        }

        String loaded = storage.getItem(KEY);
        if (loaded == null || "".equals(loaded)) {
            return null;
        }

        try {
            return DtoFactory.asDto(Dtos.FilterDataDto.class, loaded);
        } catch (Exception e) {
            return null;
        }
    }
}

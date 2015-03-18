create or replace package pkg_catalog_i is

  -- операции с каталогом

  -- просмотреть узлы на одном уровне с текущем
  function get_current_nodes(p_data xmltype) return xmltype;

  -- просмотреть всех потомков для текущего узла
  function get_child_nodes(p_data xmltype) return xmltype;

  -- получить самый подходящий узел каталога
  function get_relevant_node_fuzzy(p_data xmltype) return xmltype;

end pkg_catalog_i;
/
create or replace package body pkg_catalog_i is

  function get_current_nodes(p_data xmltype) return xmltype is
    l_result    xmltype;
    l_node_id   catalog.node_id%type;
    l_node_id_s catalog_secondary.node_id%type;
  begin
    select extractvalue(p_data, '/parameters/node_id'),
           -- фикс бага в js (получение родственников)
           decode(extractvalue(p_data, '/parameters/node_id_s'),
                  0,
                  null,
                  extractvalue(p_data, '/parameters/node_id_s'))
      into l_node_id, l_node_id_s
      from dual;

    if (l_node_id_s is not null) then
      select xmlelement("nodes",
                        xmlagg(xmlelement("node",
                                          xmlforest(l_node_id "node_id",
                                                    c.node_id "node_id_s",
                                                    c.node_name "node_name",
                                                    decode(c.left_index_id,
                                                           c.right_index_id - 1,
                                                           1,
                                                           0) "is_leaf"))
                               order by c.node_name))
        into l_result
        from catalog_secondary c
       where c.parent_node_id =
             (select c.parent_node_id
                from catalog_secondary c
               where c.node_id = l_node_id_s)
         and c.node_id != l_node_id_s;
    else
      select xmlelement("nodes",
                        xmlagg(xmlelement("node",
                                          xmlforest(c.node_id "node_id",
                                                    c.node_name "node_name",
                                                    decode(c.left_index_id,
                                                           c.right_index_id - 1,
                                                           1,
                                                           0) "is_leaf"))
                               order by c.node_name))
        into l_result
        from catalog c
       where c.parent_node_id =
             (select c.parent_node_id
                from catalog c
               where c.node_id = l_node_id)
         and c.node_id != l_node_id;
    end if;

    return l_result;
  end;

  function get_child_nodes(p_data xmltype) return xmltype is
    l_result    xmltype;
    l_node_id   catalog.node_id%type;
    l_node_id_s catalog_secondary.node_id%type;
    l_is_leaf   number;
  begin
    select extractvalue(p_data, '/parameters/node_id'),
           extractvalue(p_data, '/parameters/node_id_s'),
           extractvalue(p_data, '/parameters/is_leaf')
      into l_node_id, l_node_id_s, l_is_leaf
      from dual;

    if (l_node_id_s is not null or l_is_leaf = 1) then
      select xmlelement("nodes",
                        xmlagg(xmlelement("node",
                                          xmlforest(l_node_id "node_id",
                                                    c.node_id "node_id_s",
                                                    c.node_name "node_name",
                                                    decode(c.left_index_id,
                                                           c.right_index_id - 1,
                                                           1,
                                                           0) "is_leaf"))
                               order by c.node_name))
        into l_result
        from catalog_secondary c
       where c.parent_node_id = coalesce(l_node_id_s, 0);
    else
      select xmlelement("nodes",
                        xmlagg(xmlelement("node",
                                          xmlforest(c.node_id "node_id",
                                                    c.node_name "node_name",
                                                    decode(c.left_index_id,
                                                           c.right_index_id - 1,
                                                           1,
                                                           0) "is_leaf"))
                               order by c.node_name))
        into l_result
        from catalog c
       where c.parent_node_id = l_node_id;
    end if;

    return l_result;
  end;

  function get_relevant_node_fuzzy(p_data xmltype) return xmltype is
    l_query            varchar2(500 char);
    l_node_id          catalog.node_id%type;
    l_node_id_s        catalog_secondary.node_id%type;
    l_left_index_id    catalog.left_index_id%type;
    l_right_index_id   catalog.right_index_id%type;
    l_left_index_id_s  catalog_secondary.left_index_id%type;
    l_right_index_id_s catalog_secondary.right_index_id%type;
    l_result           xmltype;
  begin
    insert_log_call(p_source_event     => '[pkg_catalog_i.get_relevant_node_fuzzy]',
                    p_source_parameter => 'xml',
                    p_request          => p_data);

    select pkg_advert_i.get_fuzzy_query(extractvalue(p_data,
                                                     '/parameters/q')),
           extractvalue(p_data, '/parameters/node_id'),
           extractvalue(p_data, '/parameters/node_id_s')
      into l_query, l_node_id, l_node_id_s
      from dual;

    if (l_query is null) then
      select xmlelement("rows", null) into l_result from dual;
      return l_result;
    end if;

    -- Получаем левый и правый индекс, если используется поиск от узла. Если нет берем индекс от корня.

    begin
      select c.left_index_id, c.right_index_id
        into l_left_index_id, l_right_index_id
        from catalog c
       where c.node_id = l_node_id;
    exception
      when no_data_found then
        l_node_id := null;
    end;

    -- Получаем левый и правый индекс для подкаталога

    begin
      select c.left_index_id, c.right_index_id
        into l_left_index_id_s, l_right_index_id_s
        from catalog_secondary c
       where c.node_id = l_node_id_s;
    exception
      when no_data_found then
        l_node_id_s := null;
    end;

    /*insert_log(p_error_code   => 'relevant',
               p_error_string => l_query || ' ' || l_left_index_id || ' ' ||
                                 l_right_index_id);*/

    -- Если был указан узел от которого делать поиск дерева, то используем поиск с левым и правым индексом
    -- Если нет, простой поиск по тексту
    -- Сделать оптимизированный один запрос не получилось
    if (l_node_id is null and l_node_id_s is null) then
      -- получаем наиболее релевантную ветку из основного каталога
      select t.node_id, /*decode(t.is_leaf, 1,*/ s.node_id /*)*/ node_id_s
        into l_node_id, l_node_id_s
        from (select t.node_id,
                     decode(t.left_index_id, t.right_index_id - 1, 1, 0) is_leaf
                from (select
                       score(1) score, t.*
                        from catalog t
                       where contains(t.key_words, l_query, 1) > 0
                       order by score(1) desc, t.lvl, length(t.key_words)) t
               where rownum <= 1) t
      -- получаем наиболее релевантную ветку из подкаталога
        left join (select t.node_id
                     from (select
                            score(1) score, t.*
                             from catalog_secondary t
                            where contains(t.key_words, l_query, 1) > 0
                            order by score(1) desc, length(t.key_words)) t
                    where rownum <= 1) s
          on 1 = 1;
    else
      select t.node_id, /*decode(t.is_leaf, 1,*/ s.node_id /*)*/ node_id_s
        into l_node_id, l_node_id_s
        from (select t.node_id,
                     decode(t.left_index_id, t.right_index_id - 1, 1, 0) is_leaf
                from (select score(1) score,
                             t.node_id,
                             t.left_index_id,
                             t.right_index_id,
                             t.lvl,
                             length(t.key_words) ln
                        from (select t.node_id,
                                     t.left_index_id,
                                     t.right_index_id,
                                     t.key_words,
                                     t.lvl
                                from catalog t
                               where t.left_index_id > l_left_index_id
                                 and t.right_index_id < l_right_index_id) t
                       where contains(t.key_words, l_query, 1) >= 0
                      -- Если ничего не найдено, то подставляем уже выбранную ветку в общем селекте
                      union
                      select 0 score,
                             t.node_id,
                             t.left_index_id,
                             t.right_index_id,
                             t.lvl,
                             length(t.key_words) ln
                        from catalog t
                       where t.node_id = l_node_id
                       order by score desc, lvl, ln) t
               where rownum <= 1) t
      -- получаем наиболее релевантную ветку из подкаталога
        left join (select t.node_id
                     from (select score(1) score, t.*
                             from (select *
                                     from catalog_secondary t
                                    where (t.left_index_id > l_left_index_id_s and
                                          t.right_index_id <
                                          l_right_index_id_s)
                                       or l_left_index_id_s is null) t
                            where contains(t.key_words, l_query, 1) > 0
                            order by score(1) desc, t.lvl, length(t.key_words)) t
                    where rownum <= 1) s
          on 1 = 1;
    end if;

    select xmlelement("rows",
                      xmlelement("row",
                                 xmlagg(xmlelement("node",
                                                   xmlforest(c.node_id
                                                             "node_id",
                                                             c.node_id_s
                                                             "node_id_s",
                                                             c.node_name
                                                             "node_name",
                                                             c.lvl "lvl",
                                                             c.is_leaf
                                                             "is_leaf"))
                                        order by c.lvl desc)))
      into l_result
    -- вычисляем общую глубину и глубуину для каждого узла нового дерева
      from (select c.node_id,
                   c.node_name,
                   decode(c.node_id_s,
                          null,
                          c.lvl + max(c.max_lvl) over(),
                          c.lvl) lvl,
                   /*coalesce(*/
                   c.node_id_s /*, decode(c.is_leaf, 1, 0, null))*/ node_id_s,
                   c.is_leaf
            -- склеиваем дерево из основного каталога и подкаталога
              from (select c.node_id,
                           c.node_name,
                           level lvl,
                           0 max_lvl,
                           null node_id_s,
                           decode(c.left_index_id, c.right_index_id - 1, 1, 0) is_leaf
                      from catalog c
                     start with c.node_id = l_node_id
                    connect by prior c.parent_node_id = c.node_id
                    union
                    select l_node_id node_id,
                           c.node_name,
                           level lvl,
                           max(level) over() max_lvl,
                           c.node_id node_id_s,
                           decode(c.left_index_id, c.right_index_id - 1, 1, 0) is_leaf
                      from catalog_secondary c
                     where c.node_id != 0
                     start with c.node_id = l_node_id_s
                    connect by prior c.parent_node_id = c.node_id) c) c;

    --insert_log_call(p_source_event => 'fuzzy', p_source_parameter => l_query, p_request => p_data, p_response => l_result);
    return l_result;
  exception
    when others then
      insert_err(p_error_code   => 'fuzzy',
                 p_error_string => sqlerrm || '   ' || l_query,
                 p_err_request  => p_data);
      -- возвращаем рутовый элемент
      select xmlelement("rows",
                        xmlelement("row",
                                   xmlelement("node",
                                              xmlforest(c.node_id "node_id",
                                                        c.node_name "node_name"))))
        into l_result
        from catalog c
       where c.node_id = 0;
      return l_result;

  end;

end pkg_catalog_i;
/

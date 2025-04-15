import os
import json
import requests
import logging
from urllib.parse import urljoin
from urllib.request import urlretrieve, urlcleanup
from functools import lru_cache
from typing import Dict, List, Optional, Union, Set
from pathvalidate import sanitize_filename
import re
from bs4 import BeautifulSoup
try:
    import html2markdown
except ImportError:
    print("请安装html2markdown包: pip install html2markdown")
    html2markdown = None

logger = logging.getLogger(__name__)


class Visitor:
    def __init__(self, token: Optional[str]):
        self.token = token
        # 添加默认请求头
        self.headers = {
            'Accept': 'application/json',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Device-Type': 'web',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
        }
        # 设置COOKIE，可选参数
        if token:
            cookie_str = f'user=%7B%22token%22%3A%22{token}%22%7D'
            self.headers['Cookie'] = cookie_str

    def get_api_response(self, uri: str, params: Optional[dict] = None):

        url = urljoin("https://api.vistopia.com.cn/api/v1/", uri)

        if params is None:
            params = {}

        # 根据不同端点使用不同参数名
        if "class/" in uri:
            params.update({"api_token": self.token})
        else:
            params.update({"api_token": self.token})

        logger.debug(f"Visiting {url}")
        logger.debug(f"Headers: {self.headers}")
        logger.debug(f"Params: {params}")

        try:
            response = requests.get(url, params=params, headers=self.headers).json()
            logger.debug(f"API Response: {json.dumps(response, ensure_ascii=False, indent=2)}")
            
            if response["status"] != "success":
                logger.error(f"API 请求失败！")
                logger.error(f"错误码: {response.get('error_code', '未知')}")
                logger.error(f"错误信息: {response.get('message', '未知错误')}")
                logger.error(f"完整响应: {json.dumps(response, ensure_ascii=False)}")
                
                # 尝试使用备用API端点
                if uri == "user/subscriptions-list":
                    logger.info("尝试使用备用API端点获取订阅列表...")
                    alt_url = "https://www.vistopia.com.cn/api/v1/class/content"
                    alt_params = {"api_token": self.token, "class_id": -1, "sort": 1, "page": 1}
                    alt_response = requests.get(alt_url, params=alt_params, headers=self.headers).json()
                    logger.debug(f"备用API响应: {json.dumps(alt_response, ensure_ascii=False, indent=2)}")
                    if alt_response["status"] == "success":
                        return alt_response["data"]
            
            assert response["status"] == "success", f"API请求失败: {response.get('message', '未知错误')}"
            assert "data" in response.keys(), "API响应中缺少'data'字段"
            
            return response["data"]
        except Exception as e:
            logger.error(f"请求失败: {str(e)}")
            raise

    @lru_cache()
    def get_catalog(self, id: int):
        response = self.get_api_response(f"content/catalog/{id}")
        return response

    @lru_cache()
    def get_user_subscriptions_list(self):
        try:
            # 尝试使用新API
            alt_url = "https://www.vistopia.com.cn/api/v1/class/content"
            alt_params = {"api_token": self.token, "class_id": -1, "sort": 1, "page": 1}
            logger.debug(f"尝试直接使用备用API获取订阅列表: {alt_url}")
            alt_response = requests.get(alt_url, params=alt_params, headers=self.headers).json()
            logger.debug(f"备用API响应: {json.dumps(alt_response, ensure_ascii=False, indent=2)}")
            
            if alt_response["status"] == "success" and "data" in alt_response:
                logger.info("成功使用备用API获取订阅列表")
                return alt_response["data"].get("data", [])
        except Exception as e:
            logger.warning(f"备用API请求失败，尝试原始方法: {str(e)}")
        
        # 如果备用API失败，回退到原始方法
        data = []
        response = self.get_api_response("user/subscriptions-list")
        data.extend(response["data"])
        return data

    @lru_cache()
    def search(self, keyword: str) -> list:
        response = self.get_api_response("search/web", {'keyword': keyword})
        return response["data"]

    @lru_cache()
    def get_content_show(self, id: int):
        response = self.get_api_response(f"content/content-show/{id}")
        return response

    def save_show(self, id: int,
                  no_tag: bool = False, no_cover: bool = False,
                  episodes: Optional[set] = None):

        from pathlib import Path

        catalog = self.get_catalog(id)
        series = self.get_content_show(id)
        catalog_title = sanitize_filename(catalog["title"])
        
        # 创建保存目录结构
        base_dir = Path("downloads")
        
        # 创建节目主目录
        show_dir = base_dir / catalog_title
        show_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建音频和文稿子目录
        audio_dir = show_dir / "audio"
        audio_dir.mkdir(exist_ok=True)
        
        print(f"开始下载《{catalog['title']}》的音频文件...")

        for part in catalog["catalog"]:
            for article in part["part"]:

                if episodes and \
                        int(article["sort_number"]) not in episodes:
                    continue

                fname = audio_dir / "{}.mp3".format(
                    sanitize_filename(article["title"])
                )
                if not fname.exists():
                    urlretrieve(article["media_key_full_url"], fname)
                    print(f"已下载音频: {fname}")

                if not no_tag:
                    self.retag(str(fname), article, catalog, series)

                if not no_cover:
                    self.retag_cover(str(fname), article, catalog, series)

    def save_transcript_html(self, id: int, episodes: Optional[set] = None):
        """
        保存节目文稿至本地（HTML格式）
        
        参数:
            id: 内容ID
            episodes: 要下载的集数集合
        """
        from pathlib import Path

        catalog = self.get_catalog(id)
        catalog_title = sanitize_filename(catalog["title"])
        
        # 创建保存目录结构
        base_dir = Path("downloads")
        
        # 创建节目主目录
        show_dir = base_dir / catalog_title
        show_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建音频和文稿子目录
        audio_dir = show_dir / "audio"
        audio_dir.mkdir(exist_ok=True)
        
        transcript_dir = show_dir / "transcript"
        transcript_dir.mkdir(exist_ok=True)
        
        print(f"开始下载《{catalog['title']}》的文稿(HTML格式)...")

        for part in catalog["catalog"]:
            for article in part["part"]:

                if episodes and \
                        int(article["sort_number"]) not in episodes:
                    continue

                fname = transcript_dir / "{}.html".format(
                    sanitize_filename(article["title"])
                )
                if not fname.exists():
                    urlretrieve(article["content_url"], fname)

                    with open(fname) as f:
                        content = f.read()

                    content = content.replace(
                        "/assets/article/course.css",
                        "https://api.vistopia.com.cn/assets/article/course.css"
                    )

                    with open(fname, "w") as f:
                        f.write(content)
                    
                    print(f"已下载文稿: {fname}")

    def save_transcript(self, id: int, episodes: Optional[set] = None, gitbook_format: bool = True):
        """
        保存节目文稿至本地（Markdown格式）
        
        参数:
            id: 内容ID
            episodes: 要下载的集数集合
            gitbook_format: 是否使用GitBook格式
        """
        from pathlib import Path
        
        if html2markdown is None:
            raise ImportError("请先安装html2markdown: pip install html2markdown")

        catalog = self.get_catalog(id)
        catalog_title = sanitize_filename(catalog["title"])
        
        # 创建保存目录结构
        base_dir = Path("downloads")
        
        # 创建节目主目录
        show_dir = base_dir / catalog_title
        show_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建音频和文稿子目录
        audio_dir = show_dir / "audio"
        audio_dir.mkdir(exist_ok=True)
        
        transcript_dir = show_dir / "transcript"
        transcript_dir.mkdir(exist_ok=True)
        
        print(f"开始下载《{catalog['title']}》的文稿(Markdown格式)...")
        
        # GitBook需要的文件
        if gitbook_format:
            # 创建GitBook所需的book.json
            book_config = {
                "title": catalog["title"],
                "description": catalog.get("subtitle", ""),
                "author": catalog.get("author", ""),
                "language": "zh-hans",
                "plugins": ["theme-default", "fontsettings"],
                "pdf": {
                    "pageNumbers": True,
                    "fontSize": 12,
                    "paperSize": "a4",
                    "margin": {
                        "right": 62,
                        "left": 62,
                        "top": 36,
                        "bottom": 36
                    }
                }
            }
            
            with open(transcript_dir / "book.json", "w", encoding="utf-8") as f:
                json.dump(book_config, f, ensure_ascii=False, indent=2)
            
            # 创建README.md作为首页
            with open(transcript_dir / "README.md", "w", encoding="utf-8") as f:
                f.write(f"# {catalog['title']}\n\n")
                if "subtitle" in catalog:
                    f.write(f"{catalog['subtitle']}\n\n")
                if "author" in catalog:
                    f.write(f"作者: {catalog['author']}\n\n")
                if "description" in catalog:
                    f.write(f"{catalog['description']}\n\n")
            
            # 创建SUMMARY.md作为目录
            with open(transcript_dir / "SUMMARY.md", "w", encoding="utf-8") as f:
                f.write(f"# 目录\n\n")
                f.write(f"* [简介](README.md)\n")
        
        # 收集要写入SUMMARY.md的目录项
        summary_items = []

        # 遍历所有分集
        for part_index, part in enumerate(catalog["catalog"]):
            # 如果有多个单元/章节，为每个单元创建目录
            part_title = part.get("title", f"第{part_index+1}章")
            part_dir = None
            
            if gitbook_format and len(catalog["catalog"]) > 1:
                part_dir = transcript_dir / sanitize_filename(part_title)
                part_dir.mkdir(exist_ok=True)
                
                # 添加章节到SUMMARY.md
                summary_items.append(f"* [{part_title}]()")
            
            for article in part["part"]:
                if episodes and int(article["sort_number"]) not in episodes:
                    continue
                
                article_id = article["article_id"]
                title = article["title"]
                safe_title = sanitize_filename(title)
                
                # 使用新API获取完整文章内容
                html_content = self.get_article_full_content(article_id)
                if not html_content:
                    print(f"警告: 无法获取文章 '{title}' 的内容")
                    continue
                
                # 转换为Markdown
                markdown_content = self.html_to_markdown(html_content)
                
                # 构建文件路径
                if part_dir and gitbook_format:
                    # 如果有单元目录，保存到单元目录下
                    file_path = part_dir / f"{safe_title}.md"
                    # 相对路径用于SUMMARY.md
                    relative_path = f"{sanitize_filename(part_title)}/{safe_title}.md"
                else:
                    # 否则直接保存到transcript目录
                    file_path = transcript_dir / f"{safe_title}.md"
                    relative_path = f"{safe_title}.md"
                
                # 保存文件
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(f"# {title}\n\n")
                    f.write(markdown_content)
                
                print(f"已下载文稿: {file_path}")
                
                # 添加到SUMMARY.md
                if gitbook_format:
                    if part_dir:
                        summary_items.append(f"  * [{title}]({relative_path})")
                    else:
                        summary_items.append(f"* [{title}]({relative_path})")
        
        # 更新SUMMARY.md
        if gitbook_format:
            with open(transcript_dir / "SUMMARY.md", "a", encoding="utf-8") as f:
                for item in summary_items:
                    f.write(f"{item}\n")
            
            print(f"GitBook格式文件已保存到: {transcript_dir}")
            print(f"可以使用 'gitbook serve {transcript_dir}' 在本地预览")
            print(f"或使用 'gitbook epub {transcript_dir}' 生成EPUB电子书")
    
    def get_article_full_content(self, article_id: str) -> str:
        """
        获取文章完整内容
        
        参数:
            article_id: 文章ID
        
        返回:
            文章HTML内容
        """
        url = "https://www.vistopia.com.cn/api/v1/reader/section-detail"
        params = {
            "api_token": self.token,
            "article_id": article_id,
            "share_uid": ""
        }
        
        headers = {
            'Accept': 'application/json',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'User-Agent': 'Mozilla/5.0'
        }
        
        if self.token:
            headers['Cookie'] = f'user=%7B%22token%22%3A%22{self.token}%22%7D'
        
        response = requests.get(url, params=params, headers=headers)
        data = response.json()
        
        if data.get("status") != "success":
            logger.error(f"获取文章内容失败: {data.get('message', '未知错误')}")
            return ""
        
        # 提取文章内容
        if "part" in data.get("data", {}) and len(data["data"]["part"]) > 0:
            return data["data"]["part"][0].get("content", "")
        
        return ""
    
    def html_to_markdown(self, html_content: str) -> str:
        """
        将HTML内容转换为Markdown格式
        保持标题层级结构，正确处理常见HTML标签
        
        参数:
            html_content: HTML内容
        
        返回:
            Markdown格式的内容
        """
        # 使用BeautifulSoup解析HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 处理标题层级
        headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        for header in headers:
            # 获取标题级别
            level = int(header.name[1])
            # 创建相应级别的Markdown标题
            header.replace_with(f"{'#' * level} {header.get_text().strip()}\n\n")
        
        # 处理段落标签
        paragraphs = soup.find_all('p')
        for p in paragraphs:
            # 将段落转换为Markdown段落
            p.replace_with(f"{p.get_text().strip()}\n\n")
        
        # 处理div标签（通常用作段落）
        divs = soup.find_all('div')
        for div in divs:
            # 将div转换为Markdown段落
            div.replace_with(f"{div.get_text().strip()}\n\n")
        
        # 处理强调标签
        strongs = soup.find_all('strong')
        for strong in strongs:
            # 将strong转换为Markdown加粗格式
            strong.replace_with(f"**{strong.get_text().strip()}**")
        
        # 处理图片
        images = soup.find_all('img')
        for img in images:
            src = img.get('src', '')
            alt = img.get('alt', '')
            img.replace_with(f"![{alt}]({src})\n\n")
        
        # 处理列表
        # 有序列表
        ols = soup.find_all('ol')
        for i, ol in enumerate(ols):
            items = ol.find_all('li')
            list_content = "\n".join([f"{j+1}. {item.get_text().strip()}" for j, item in enumerate(items)])
            ol.replace_with(f"{list_content}\n\n")
        
        # 无序列表
        uls = soup.find_all('ul')
        for ul in uls:
            items = ul.find_all('li')
            list_content = "\n".join([f"- {item.get_text().strip()}" for item in items])
            ul.replace_with(f"{list_content}\n\n")
        
        # 处理链接
        links = soup.find_all('a')
        for link in links:
            href = link.get('href', '')
            text = link.get_text().strip()
            link.replace_with(f"[{text}]({href})")
        
        # 处理引用
        blockquotes = soup.find_all('blockquote')
        for quote in blockquotes:
            # 在每行前添加>
            quote_text = quote.get_text().strip().replace('\n', '\n> ')
            quote.replace_with(f"> {quote_text}\n\n")
        
        # 处理em标签（斜体）
        ems = soup.find_all('em')
        for em in ems:
            em.replace_with(f"*{em.get_text().strip()}*")
            
        # 处理code标签（行内代码）
        codes = soup.find_all('code')
        for code in codes:
            code.replace_with(f"`{code.get_text().strip()}`")
        
        # 处理pre标签（代码块）
        pres = soup.find_all('pre')
        for pre in pres:
            pre_text = pre.get_text().strip()
            pre.replace_with(f"```\n{pre_text}\n```\n\n")
        
        # 获取处理后的HTML
        html_processed = str(soup)
        
        # 使用html2markdown进行最终转换
        try:
            markdown_content = html2markdown.convert(html_processed)
        except Exception as e:
            logger.warning(f"html2markdown转换失败: {e}，使用自定义转换")
            # 自定义转换逻辑，移除所有剩余的HTML标签
            markdown_content = html_processed
        
        # 清理多余的HTML标签
        markdown_content = re.sub(r'<[^>]*>', '', markdown_content)
        
        # 清理额外的空行
        markdown_content = re.sub(r'\n{3,}', '\n\n', markdown_content)
        
        return markdown_content

    def save_transcript_with_single_file(self, id: int,
                                         episodes: Optional[set] = None,
                                         single_file_exec_path: str = "",
                                         cookie_file_path: str = ""):
        import subprocess
        from pathlib import Path
        logger.debug(f"save_transcript_with_single_file id {id}")

        catalog = self.get_catalog(id)
        catalog_title = sanitize_filename(catalog["title"])
        
        # 创建保存目录结构
        base_dir = Path("downloads")
        
        # 创建节目主目录
        show_dir = base_dir / catalog_title
        show_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建音频和文稿子目录
        audio_dir = show_dir / "audio"
        audio_dir.mkdir(exist_ok=True)
        
        transcript_dir = show_dir / "transcript"
        transcript_dir.mkdir(exist_ok=True)

        for part in catalog["catalog"]:
            for article in part["part"]:
                if episodes and int(article["sort_number"]) not in episodes:
                    continue

                fname = transcript_dir / "{}.html".format(
                    sanitize_filename(article["title"])
                )
                if not fname.exists():
                    command = [
                        single_file_exec_path,
                        "https://www.vistopia.com.cn/article/"
                        + article["article_id"],
                        str(fname),
                        "--browser-cookies-file=" + cookie_file_path
                    ]
                    logger.debug(f"singlefile command {command}")
                    try:
                        subprocess.run(command, check=True)
                        print(
                            f"已下载文稿: {fname}")
                    except subprocess.CalledProcessError as e:
                        print(f"Failed to fetch page using single-file: {e}")

    @staticmethod
    def retag(
        fname: str,
        article_info: dict,
        catalog_info: dict,
        series_info: dict
    ):

        from mutagen.easyid3 import EasyID3
        from mutagen.id3 import ID3NoHeaderError

        try:
            track = EasyID3(fname)
        except ID3NoHeaderError:
            # No ID3 tag found, creating a new ID3 tag
            # See: https://github.com/quodlibet/mutagen/issues/327
            track = EasyID3()

        track['title'] = article_info['title']
        track['album'] = series_info['title']
        track['artist'] = series_info['author']
        track['tracknumber'] = str(article_info['sort_number'])
        track['website'] = article_info['content_url']

        try:
            track.save(fname)
        except Exception as e:
            print(f"Error saving ID3 tags: {e}")

    @staticmethod
    def retag_cover(fname, article_info, catalog_info, series_info):

        from mutagen.id3 import ID3, APIC

        @lru_cache()
        def _get_cover(url: str) -> bytes:
            cover_fname, _ = urlretrieve(url)
            with open(cover_fname, "rb") as fp:
                cover = fp.read()
            urlcleanup()
            return cover

        cover = _get_cover(catalog_info["background_img"])

        track = ID3(fname)
        track["APIC"] = APIC(encoding=3, mime="image/jpeg",
                             type=3, desc="Cover", data=cover)
        track.save()

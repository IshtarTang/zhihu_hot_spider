/*
Navicat MySQL Data Transfer

Source Server         : mysql
Source Server Version : 50527
Source Host           : localhost:3306
Source Database       : zhihu

Target Server Type    : MYSQL
Target Server Version : 50527
File Encoding         : 65001

Date: 2022-02-15 18:18:30
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for `answers`
-- ----------------------------
DROP TABLE IF EXISTS `answers`;
CREATE TABLE `answers` (
`answer_url`  varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL ,
`question_id`  varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL ,
`content`  mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL ,
`voteup_count`  int(11) NULL DEFAULT NULL ,
`external_links`  text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL ,
`img_list`  text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL ,
`created_time`  int(11) NULL DEFAULT NULL ,
`update_time`  int(11) NULL DEFAULT NULL ,
`download_time`  int(11) NULL DEFAULT NULL ,
`comment_count`  int(11) NULL DEFAULT NULL ,
`author_name`  varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL ,
`is_advertiser`  varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL ,
`author_introduction`  text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL ,
`auhtor_user_url`  varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL ,
`autho_gender`  int(11) NULL DEFAULT NULL ,
PRIMARY KEY (`answer_url`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_general_ci

;

-- ----------------------------
-- Table structure for `hot_rank`
-- ----------------------------
DROP TABLE IF EXISTS `hot_rank`;
CREATE TABLE `hot_rank` (
`hot_ranking`  int(11) NULL DEFAULT NULL ,
`d_timestamp`  int(11) NULL DEFAULT NULL ,
`hot_metrics`  varchar(30) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' ,
`title`  varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL ,
`link`  varchar(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' ,
`excerpt`  text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL ,
PRIMARY KEY (`link`, `hot_metrics`)
)
ENGINE=InnoDB
DEFAULT CHARACTER SET=utf8mb4 COLLATE=utf8mb4_general_ci

;
